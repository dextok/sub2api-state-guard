package ticketpool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

// directClients 把所有探针都发到本地桩上，不经代理、不出网。
type directClients struct{ client *http.Client }

func (c directClients) Client(string) (*http.Client, error) { return c.client, nil }

// captureStub 是 codex 网关的假桩，按测试设定回状态码、state 头与 SSE 正文。
type captureStub struct {
	server *httptest.Server

	mu     sync.Mutex
	status int
	state  string
	body   string
	// echoModel 为真时正文由假桩现编：自报的模型就是请求里的那个模型，
	// 也就是上游正常服务时的样子（满血判定要求两者一致）。
	echoModel bool

	// 最后一次收到的请求，用于断言探针发了什么。
	lastPath   string
	lastHeader http.Header
	lastBody   map[string]any
}

func newCaptureStub(t *testing.T) *captureStub {
	t.Helper()
	stub := &captureStub{status: http.StatusOK}
	stub.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(request.Body, 1<<20))

		stub.mu.Lock()
		stub.lastPath = request.URL.Path
		stub.lastHeader = request.Header.Clone()
		stub.lastBody = nil
		json.Unmarshal(raw, &stub.lastBody)
		status, state, body := stub.status, stub.state, stub.body
		if stub.echoModel {
			served, _ := stub.lastBody["model"].(string)
			body = sseCreatedAs(served) + sseText
		}
		stub.mu.Unlock()

		if state != "" {
			writer.Header().Set(pluginconfig.DefaultHeaderName, state)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(status)
		io.WriteString(writer, body)
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

// reply 设定下一次响应的内容。
func (s *captureStub) reply(status int, state, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.state, s.body = status, state, body
	s.echoModel = false
}

// replyServed 回一个「上游照请求的模型服务」的正常响应，不管探的是哪个模型。
func (s *captureStub) replyServed(status int, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.state, s.body = status, state, ""
	s.echoModel = true
}

// seen 返回最后一次收到的请求。
func (s *captureStub) seen() (string, http.Header, map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPath, s.lastHeader, s.lastBody
}

const (
	sseText      = "event: response.output_text.delta\ndata: {\"delta\":\"ok\"}\n\n"
	sseCompleted = "event: response.completed\ndata: {\"response\":{\"id\":\"r1\"}}\n\n"
	sseFailed    = "event: response.failed\ndata: {\"response\":{\"error\":{\"code\":\"x\"}}}\n\n"
	sseOverload  = "event: error\ndata: {\"error\":{\"code\":\"model_overloaded\",\"message\":\"try later\"}}\n\n"
	sseOther     = "event: error\ndata: {\"error\":{\"code\":\"rate_limit\",\"message\":\"slow down\"}}\n\n"
)

func state(length int) string { return strings.Repeat("s", length) }

// probeModel 是 probeOnce 请求的模型；满血判定要求上游自报的就是它。
const probeModel = "gpt-5-codex"

// sseServed 是一条正常的流：上游自报的模型与请求的一致，并且有产出。
var sseServed = sseCreatedAs(probeModel) + sseText

// sseCreatedAs / sseCompletedAs 让假桩自报一个模型名，用来模拟上游换模型服务。
func sseCreatedAs(model string) string {
	return "event: response.created\ndata: {\"response\":{\"id\":\"r1\",\"model\":" +
		jsonString(model) + "}}\n\n"
}

func sseCompletedAs(model string) string {
	return "event: response.completed\ndata: {\"response\":{\"id\":\"r1\",\"model\":" +
		jsonString(model) + "}}\n\n"
}

func jsonString(value string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func newProber(t *testing.T, stub *captureStub, tune func(*pluginconfig.Ticket)) *prober {
	t.Helper()
	config := pluginconfig.DefaultTicketPool()
	config.GatewayBaseURL = stub.server.URL + "/backend-api/codex"
	config.ProbeTimeoutSeconds = 5
	if tune != nil {
		tune(&config)
	}
	return &prober{
		clients: directClients{client: stub.server.Client()},
		config:  config,
		header:  pluginconfig.DefaultHeaderName,
	}
}

func probeOnce(t *testing.T, stub *captureStub, cred Credential, tune func(*pluginconfig.Ticket)) record {
	t.Helper()
	return newProber(t, stub, tune).probe(context.Background(), cred, "gpt-5-codex", "")
}

// noLength 关掉「满血票长度」这道可选的附加条件，只留「200 + 模型一致」的基本判定。
func noLength(config *pluginconfig.Ticket) { config.TargetStateLength = 0 }

func testCredential() Credential {
	return Credential{Authorization: "Bearer " + strings.Repeat("a", 40), AccountHeader: "acct-9"}
}

// 分级表决定了哪张票入池，只有 healthy 会入池：200 + 有产出 + 上游自报的模型与请求的一致。
func TestProbeGrading(t *testing.T) {
	cases := []struct {
		name   string
		status int
		state  string
		body   string
		want   Grade
	}{
		{"满血", 200, state(292), sseServed, GradeHealthy},
		{"只有 completed 也算有产出", 200, state(292), sseCompletedAs(probeModel), GradeHealthy},
		{"没设满血票长度时长度不标准也照收", 200, state(7), sseServed, GradeHealthy},
		{"上游没回报模型名", 200, state(292), sseText, GradeUnverified},
		{"上游换了模型", 200, state(292), sseCreatedAs("gpt-5.6-luna") + sseText, GradeDowngraded},
		{"降智", 200, state(292), sseOverload, GradeOverloaded},
		{"非 overload 的错误算未完成", 200, state(292), sseOther, GradePartial},
		{"completed 里带 error 算未完成", 200, state(292), sseFailed, GradePartial},
		{"既无产出也未完成", 200, state(292), "", GradePartial},
		{"非 200 但带回了票", 429, state(292), "too many requests", GradeWeak},
		{"非 200 且没有票", 403, "", "forbidden", GradeBlocked},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newCaptureStub(t)
			stub.reply(testCase.status, testCase.state, testCase.body)

			got := probeOnce(t, stub, testCredential(), noLength)
			if got.grade != testCase.want {
				t.Fatalf("分级 = %s，期望 %s（detail=%q）", got.grade, testCase.want, got.detail)
			}
			if got.status != testCase.status {
				t.Fatalf("状态码 = %d", got.status)
			}
			if got.length != len(testCase.state) {
				t.Fatalf("长度 = %d，期望 %d", got.length, len(testCase.state))
			}
		})
	}
}

// 设了「满血票长度」时，它是模型判定之外的附加条件：模型对得上但长度不符照样不入池。
func TestProbeChecksStateLengthWhenSet(t *testing.T) {
	stub := newCaptureStub(t)
	prober := newProber(t, stub, func(config *pluginconfig.Ticket) { config.TargetStateLength = 312 })

	stub.reply(http.StatusOK, state(312), sseCreatedAs("gpt-6-astra")+sseText)
	if got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", ""); got.grade != GradeHealthy {
		t.Fatalf("长度正好对上应当是 healthy，得到 %s（detail=%q）", got.grade, got.detail)
	}

	stub.reply(http.StatusOK, state(292), sseCreatedAs("gpt-6-astra")+sseText)
	got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", "")
	if got.grade != GradeMismatch {
		t.Fatalf("长度不符应当是 mismatch，得到 %s（detail=%q）", got.grade, got.detail)
	}
	if !strings.Contains(got.detail, "292") || !strings.Contains(got.detail, "312") {
		t.Fatalf("detail 应当同时给出实际长度与设定值，得到 %q", got.detail)
	}
	if got.length != 292 {
		t.Fatalf("长度仍然要如实记录，得到 %d", got.length)
	}
	// 只有 healthy 入池。
	store := NewStore()
	if added := store.StoreRound(1, "gpt-6-astra", []record{got}, storeParams(3), time.Unix(1700000000, 0)); added != 0 {
		t.Fatalf("长度不符的票入池了 %d 张", added)
	}

	// 模型先判：模型都不对时，报「降级」比报「长度不符」更能说明问题。
	stub.reply(http.StatusOK, state(292), sseCreatedAs("gpt-5.6-luna")+sseText)
	if got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", ""); got.grade != GradeDowngraded {
		t.Fatalf("模型与长度都不对时应当报 downgraded，得到 %s", got.grade)
	}
}

// 「满血票长度」留空（0）时 state 的长度不参与判定：长度因模型、因上游版本而异，
// 只要模型对得上就是满血票。
func TestProbeIgnoresStateLengthWhenUnset(t *testing.T) {
	stub := newCaptureStub(t)
	prober := newProber(t, stub, noLength)

	for _, length := range []int{1, 292, 312, 1024} {
		stub.reply(http.StatusOK, state(length), sseCreatedAs("gpt-6-astra")+sseText)
		got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", "")
		if got.grade != GradeHealthy {
			t.Fatalf("长度 %d 的票分级 = %s，期望 healthy（detail=%q）", length, got.grade, got.detail)
		}
		if got.length != length {
			t.Fatalf("长度仍然要如实记录，得到 %d", got.length)
		}
	}
}

// 账号被降级时上游用别的模型服务，铸出来的 state 长度可能正好等于目标值。
// 光看长度分不出来，必须看上游自报的模型名，否则这张票会被当成满血票注给下一个请求。
func TestProbeDetectsServedModelDowngrade(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(312), sseCreatedAs("gpt-5.6-luna")+sseText)
	prober := newProber(t, stub, noLength)

	got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", "")
	if got.grade != GradeDowngraded {
		t.Fatalf("分级 = %s，期望 downgraded（detail=%q）", got.grade, got.detail)
	}
	if !strings.Contains(got.detail, "gpt-5.6-luna") || !strings.Contains(got.detail, "gpt-6-astra") {
		t.Fatalf("detail 应当点明是谁替谁服务的，得到 %q", got.detail)
	}
	// 只有 healthy 入池，降级票必须被挡在池外。
	store := NewStore()
	if added := store.StoreRound(1, "gpt-6-astra", []record{got}, storeParams(3), time.Unix(1700000000, 0)); added != 0 {
		t.Fatalf("降级票入池了 %d 张", added)
	}

	// response.completed 里的模型名同样算数（有些响应不带 created 事件）。
	stub.reply(http.StatusOK, state(312), sseCompletedAs("gpt-5.6-luna"))
	if got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", ""); got.grade != GradeDowngraded {
		t.Fatalf("completed 里的模型名也要认，得到 %s", got.grade)
	}

	// 上游自报的名字与请求一致才是满血票。
	stub.reply(http.StatusOK, state(312), sseCreatedAs("gpt-6-astra")+sseText)
	if got := prober.probe(context.Background(), testCredential(), "gpt-6-astra", ""); got.grade != GradeHealthy {
		t.Fatalf("同名时应当是 healthy，得到 %s（detail=%q）", got.grade, got.detail)
	}
	// 上游一个模型名都没给：确认不了这张票属于谁，宁可不收，也不能当成满血票。
	stub.reply(http.StatusOK, state(312), sseText)
	got = prober.probe(context.Background(), testCredential(), "gpt-6-astra", "")
	if got.grade != GradeUnverified {
		t.Fatalf("没有模型名时应当是 unverified，得到 %s", got.grade)
	}
	if added := store.StoreRound(1, "gpt-6-astra", []record{got}, storeParams(3), time.Unix(1700000000, 0)); added != 0 {
		t.Fatalf("未确认的票入池了 %d 张", added)
	}
}

// detail 会经看板回到浏览器，上游返回的模型名是外部输入：认不出来就不回显原文。
func TestProbeDoesNotEchoUnsafeServedModel(t *testing.T) {
	stub := newCaptureStub(t)
	evil := "<img src=x onerror=alert(1)>"
	stub.reply(http.StatusOK, state(292), sseCreatedAs(evil)+sseText)

	got := probeOnce(t, stub, testCredential(), nil)
	if got.grade != GradeDowngraded {
		t.Fatalf("分级 = %s，期望 downgraded", got.grade)
	}
	if strings.Contains(got.detail, "<img") || strings.Contains(got.detail, evil) {
		t.Fatalf("非法模型名被原样回显到 detail: %q", got.detail)
	}
}

// state 会被原样写进发往上游的请求头，含 CR/LF/NUL 就是响应头注入。
func TestProbeRejectsUnsafeState(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, "", sseServed)
	// httptest 的 ResponseWriter 会挡掉真正的 CRLF，这里直接单测 sanitizeState。
	for _, value := range []string{"a\rb", "a\nb", "a\x00b", strings.Repeat("s", maxStateBytes+1), ""} {
		if got := sanitizeState(value); got != "" {
			t.Fatalf("sanitizeState(%d 字节) = %q，期望被丢弃", len(value), got)
		}
	}
	if got := sanitizeState("ok"); got != "ok" {
		t.Fatalf("正常值被改写成 %q", got)
	}

	// 头里带控制字符时整张票作废：没有 state 的票分级可以是 healthy，但 StoreRound 不收。
	got := probeOnce(t, stub, testCredential(), nil)
	if got.state != "" {
		t.Fatalf("没有 state 时 = %q", got.state)
	}
	if added := NewStore().StoreRound(1, probeModel, []record{got}, storeParams(3), time.Unix(1700000000, 0)); added != 0 {
		t.Fatalf("没有 state 的票入池了 %d 张", added)
	}
}

// 网关根跟着宿主实际在用的端点走：换中转、换域名都不需要改插件配置。
func TestProbeUsesHarvestedGatewayAndUserAgent(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(292), sseServed)

	cred := testCredential()
	cred.GatewayBase = stub.server.URL + "/relay/v1/"
	cred.UserAgent = "codex_cli_rs/9.9.9"

	got := probeOnce(t, stub, cred, func(config *pluginconfig.Ticket) {
		config.GatewayBaseURL = "https://never.example.com/backend-api/codex"
		config.UserAgent = "fallback-ua"
	})
	if got.grade != GradeHealthy {
		t.Fatalf("分级 = %s（detail=%q）", got.grade, got.detail)
	}
	path, header, _ := stub.seen()
	if path != "/relay/v1/responses" {
		t.Fatalf("探针打到了 %q，期望采到的网关根 + /responses", path)
	}
	if ua := header.Get("User-Agent"); ua != "codex_cli_rs/9.9.9" {
		t.Fatalf("UA = %q，期望沿用真实客户端的", ua)
	}
	if auth := header.Get("Authorization"); auth != cred.Authorization {
		t.Fatalf("Authorization = %q", auth)
	}
	if account := header.Get("ChatGpt-Account-Id"); account != "acct-9" {
		t.Fatalf("账号头 = %q", account)
	}
	if accept := header.Get("Accept"); accept != "text/event-stream" {
		t.Fatalf("Accept = %q", accept)
	}
}

func TestProbeFallsBackToConfiguredGatewayAndUserAgent(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(292), sseServed)

	got := probeOnce(t, stub, testCredential(), func(config *pluginconfig.Ticket) {
		config.UserAgent = "fallback-ua"
	})
	if got.grade != GradeHealthy {
		t.Fatalf("分级 = %s", got.grade)
	}
	path, header, _ := stub.seen()
	if path != "/backend-api/codex/responses" {
		t.Fatalf("探针打到了 %q", path)
	}
	if ua := header.Get("User-Agent"); ua != "fallback-ua" {
		t.Fatalf("UA = %q", ua)
	}
}

// 探针请求体越小越省 token；effort=none 时连 reasoning 字段都不带。
func TestProbeBody(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(292), sseText)

	probeOnce(t, stub, testCredential(), func(config *pluginconfig.Ticket) {
		config.ProbeEffort = pluginconfig.EffortLow
	})
	_, _, body := stub.seen()
	if body["model"] != "gpt-5-codex" || body["stream"] != true {
		t.Fatalf("请求体 = %v", body)
	}
	if body["store"] != false {
		t.Fatalf("store 应为 false，避免在上游留下记录：%v", body["store"])
	}
	reasoning, ok := body["reasoning"].(map[string]any)
	if !ok || reasoning["effort"] != pluginconfig.EffortLow {
		t.Fatalf("reasoning = %v", body["reasoning"])
	}

	probeOnce(t, stub, testCredential(), func(config *pluginconfig.Ticket) {
		config.ProbeEffort = pluginconfig.EffortNone
	})
	if _, _, body = stub.seen(); body["reasoning"] != nil {
		t.Fatalf("effort=none 时不该带 reasoning：%v", body)
	}
}

// 连响应都没拿到：代理或网络问题，不是账号问题，记 error 且不入池。
func TestProbeReportsTransportError(t *testing.T) {
	stub := newCaptureStub(t)
	stub.server.Close()

	got := probeOnce(t, stub, testCredential(), nil)
	if got.grade != GradeError {
		t.Fatalf("分级 = %s", got.grade)
	}
	if got.state != "" || got.status != 0 {
		t.Fatalf("记录 = %+v", got)
	}
	if got.detail == "" {
		t.Fatal("应当留下失败摘要")
	}
}

// 连响应都没拿到时，错误文本来自 net/http 与代理库，里面会带网关地址、代理 host:port；
// 它们要进看板，所以必须先打码。
func TestProbeErrorDetailMasksAddresses(t *testing.T) {
	stub := newCaptureStub(t)
	address := strings.TrimPrefix(stub.server.URL, "http://")
	stub.server.Close()

	got := probeOnce(t, stub, testCredential(), nil)
	if got.grade != GradeError || got.detail == "" {
		t.Fatalf("记录 = %+v", got)
	}
	if strings.Contains(got.detail, address) {
		t.Fatalf("失败摘要回显了网关地址 %s: %s", address, got.detail)
	}
}

func TestScrubDetail(t *testing.T) {
	cases := map[string]string{
		`Post "https://chatgpt.com/backend-api/codex/responses": proxyconnect tcp: dial tcp 10.0.0.9:8080: connect: connection refused`: `Post "https://ch***": proxyconnect tcp: dial tcp 10.0.*.*:8080: connect: connection refused`,
		`socks connect tcp 1.2.3.4:1080->chatgpt.com:443: dial tcp 1.2.3.4:1080: i/o timeout`:                                           `socks connect tcp 1.2.*.*:1080->ch***:443: dial tcp 1.2.*.*:1080: i/o timeout`,
		`dial socks5://user:s3cret@proxy.example.com:1080: EOF`:                                                                         `dial socks5://pr***:1080: EOF`,
		`dial tcp [2001:db8::1]:1080: no route to host`:                                                                                 `dial tcp 20***:1080: no route to host`,
		// 点分标识符不带端口就不是地址，不能被误伤。
		`unexpected event response.output_text.delta`: `unexpected event response.output_text.delta`,
	}
	for input, want := range cases {
		if got := scrubDetail(input); got != want {
			t.Errorf("scrubDetail(%q)\n = %q\n期望 %q", input, got, want)
		}
	}
	if got := scrubDetail(`socks5://user:s3cret@1.2.3.4:1080`); strings.Contains(got, "s3cret") || strings.Contains(got, "1.2.3.4") {
		t.Fatalf("打码后仍含凭据或完整地址: %q", got)
	}
}

func TestSortRecordsRanksHealthyFirst(t *testing.T) {
	records := []record{
		{grade: GradeError, state: ""}, // 没有 state，直接被丢掉
		{grade: GradeBlocked, state: "b", length: 1},
		{grade: GradeUnverified, state: "m1", length: 312},
		{grade: GradeUnverified, state: "m2", length: 400},
		{grade: GradeHealthy, state: "h", length: 292},
		{grade: GradeWeak, state: "w", length: 292},
	}
	got := sortRecords(records)

	want := []Grade{GradeHealthy, GradeWeak, GradeUnverified, GradeUnverified, GradeBlocked}
	if len(got) != len(want) {
		t.Fatalf("排序后 %d 条，期望 %d 条（无 state 的应被丢弃）", len(got), len(want))
	}
	for index, grade := range want {
		if got[index].grade != grade {
			t.Fatalf("第 %d 条是 %s，期望 %s", index, got[index].grade, grade)
		}
	}
	// 同分级按长度降序：同样是 unverified，先试更长的那张。
	if got[2].length != 400 || got[3].length != 312 {
		t.Fatalf("同分级未按长度降序: %d, %d", got[2].length, got[3].length)
	}
}

// 一轮没补到票时这行会进看板，但它不能含 state 本身，代理地址也要打码。
func TestSummarizeRecordsMasksProxyAndOmitsState(t *testing.T) {
	got := summarizeRecords([]record{
		{grade: GradeDowngraded, state: "super-secret-state", length: 312, status: 200},
		{grade: GradeHealthy, state: "another-secret", length: 292, status: 200, proxy: "socks5://1.2.3.4:1080"},
	})
	if got != "direct=downgraded(200/312) socks5://1.2.*.*:1080=healthy(200/292)" {
		t.Fatalf("摘要 = %q", got)
	}
	if strings.Contains(got, "secret") || strings.Contains(got, "1.2.3.4") {
		t.Fatalf("摘要泄露了 state 或完整代理地址: %q", got)
	}

	// 代理可能带内联凭据，摘要同样一个字都不能带出来。
	got = summarizeRecords([]record{
		{grade: GradeHealthy, state: "s", length: 292, status: 200, proxy: "http://user:s3cr3t@10.0.0.9:8080"},
	})
	if strings.Contains(got, "s3cr3t") || strings.Contains(got, "user") {
		t.Fatalf("摘要泄露了代理凭据: %q", got)
	}
}

// maskAddr 只是 proxypool.MaskProxy 的转发：看板与日志共用一套写法，
// 这里只确认它吃的是完整代理 URL，而不是早先的裸 host:port。
func TestMaskAddr(t *testing.T) {
	cases := map[string]string{
		"socks5://1.2.3.4:1080":   "socks5://1.2.*.*:1080",
		"socks5h://10.0.0.1:9050": "socks5h://10.0.*.*:9050",
		"http://u:p@1.2.3.4:8080": "http://1.2.*.*:8080",
		"garbage":                 "***",
	}
	for addr, want := range cases {
		if got := maskAddr(addr); got != want {
			t.Errorf("maskAddr(%q) = %q，期望 %q", addr, got, want)
		}
	}
}

func TestTruncateDetail(t *testing.T) {
	if got := truncateDetail("line one\r\nline\ttwo   three"); got != "line one line two three" {
		t.Fatalf("清洗结果 = %q", got)
	}
	long := truncateDetail(strings.Repeat("x", maxDetailBytes+50))
	if !strings.HasSuffix(long, "…") || len(long) > maxDetailBytes+4 {
		t.Fatalf("截断结果长度 = %d", len(long))
	}
}

// 分级一确认就停读，省掉后续 token。
func TestReadSSEStopsAtFirstDecisiveEvent(t *testing.T) {
	stream := sseText + "event: response.completed\ndata: {\"response\":{\"error\":{}}}\n\n"
	got := readSSE(strings.NewReader(stream))
	if !got.hasText || got.completed {
		t.Fatalf("应当在首个 delta 处停下: %+v", got)
	}

	got = readSSE(strings.NewReader("data: not-json\n\n" + sseOverload + sseText))
	if !got.overloaded || got.errorMessage != "try later" {
		t.Fatalf("应当在 overload 处停下: %+v", got)
	}
}
