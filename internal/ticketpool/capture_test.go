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

func testCredential() Credential {
	return Credential{Authorization: "Bearer " + strings.Repeat("a", 40), AccountHeader: "acct-9"}
}

// 分级表决定了哪张票入池。只有 healthy 会入池，312（premium 池）已验收确认降智。
func TestProbeGrading(t *testing.T) {
	cases := []struct {
		name   string
		status int
		state  string
		body   string
		want   Grade
	}{
		{"满血", 200, state(292), sseText, GradeHealthy},
		{"只有 completed 也算有产出", 200, state(292), sseCompleted, GradeHealthy},
		{"长度不符", 200, state(312), sseText, GradeMismatch},
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

			got := probeOnce(t, stub, testCredential(), nil)
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

// target_state_length 设为 0 表示不按长度判定，任何带产出的票都入池。
func TestProbeWithoutLengthJudgement(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(312), sseText)

	got := probeOnce(t, stub, testCredential(), func(config *pluginconfig.Ticket) {
		config.TargetStateLength = 0
	})
	if got.grade != GradeHealthy {
		t.Fatalf("分级 = %s，期望 healthy", got.grade)
	}
}

// state 会被原样写进发往上游的请求头，含 CR/LF/NUL 就是响应头注入。
func TestProbeRejectsUnsafeState(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, "", sseText)
	// httptest 的 ResponseWriter 会挡掉真正的 CRLF，这里直接单测 sanitizeState。
	for _, value := range []string{"a\rb", "a\nb", "a\x00b", strings.Repeat("s", maxStateBytes+1), ""} {
		if got := sanitizeState(value); got != "" {
			t.Fatalf("sanitizeState(%d 字节) = %q，期望被丢弃", len(value), got)
		}
	}
	if got := sanitizeState("ok"); got != "ok" {
		t.Fatalf("正常值被改写成 %q", got)
	}

	// 头里带控制字符时整张票作废：没有 state 的票不可能入池。
	got := probeOnce(t, stub, testCredential(), nil)
	if got.state != "" || got.grade != GradeMismatch {
		t.Fatalf("没有 state 时 = %s/%q，期望 mismatch（长度 0 不等于 292）", got.grade, got.state)
	}
}

// 网关根跟着宿主实际在用的端点走：换中转、换域名都不需要改插件配置。
func TestProbeUsesHarvestedGatewayAndUserAgent(t *testing.T) {
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(292), sseText)

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
	stub.reply(http.StatusOK, state(292), sseText)

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
		{grade: GradeMismatch, state: "m1", length: 312},
		{grade: GradeMismatch, state: "m2", length: 400},
		{grade: GradeHealthy, state: "h", length: 292},
		{grade: GradeWeak, state: "w", length: 292},
	}
	got := sortRecords(records)

	want := []Grade{GradeHealthy, GradeWeak, GradeMismatch, GradeMismatch, GradeBlocked}
	if len(got) != len(want) {
		t.Fatalf("排序后 %d 条，期望 %d 条（无 state 的应被丢弃）", len(got), len(want))
	}
	for index, grade := range want {
		if got[index].grade != grade {
			t.Fatalf("第 %d 条是 %s，期望 %s", index, got[index].grade, grade)
		}
	}
	// 同分级按长度降序：同样是 mismatch，先试更长的那张。
	if got[2].length != 400 || got[3].length != 312 {
		t.Fatalf("同分级未按长度降序: %d, %d", got[2].length, got[3].length)
	}
}

// 一轮没补到票时这行会进看板，但它不能含 state 本身，代理地址也要打码。
func TestSummarizeRecordsMasksProxyAndOmitsState(t *testing.T) {
	got := summarizeRecords([]record{
		{grade: GradeMismatch, state: "super-secret-state", length: 312, status: 200},
		{grade: GradeHealthy, state: "another-secret", length: 292, status: 200, proxy: "socks5://1.2.3.4:1080"},
	})
	if got != "direct=mismatch(200/312) socks5://1.2.*.*:1080=healthy(200/292)" {
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
