package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

// fakeValues 模拟调度器：state 按模型存放，取值必须带模型名。
type fakeValues struct {
	enabled map[int64]bool
	// values 是「账号 → 模型名 → state」。
	values map[int64]map[string]string
	// ready 覆盖默认推断（默认：该账号有任意一个模型的 state 就算 ready）。
	ready map[int64]bool

	mu       sync.Mutex
	observed []string
	// harvested 记录每次 Harvest 拿到的 Authorization，用来确认采集发生在注入之前。
	harvested []string
}

func (f *fakeValues) Enabled(accountID int64) bool { return f.enabled[accountID] }

func (f *fakeValues) Ready(accountID int64) bool {
	if ready, ok := f.ready[accountID]; ok {
		return ready
	}
	return len(f.values[accountID]) > 0
}

func (f *fakeValues) Value(accountID int64, model string) (string, bool) {
	value, ok := f.values[accountID][model]
	return value, ok
}

func (f *fakeValues) Observe(accountID int64, model string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, model)
}

func (f *fakeValues) Harvest(_ int64, header http.Header, _ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.harvested = append(f.harvested, header.Get("Authorization"))
}

func (f *fakeValues) observedModels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.observed...)
	sort.Strings(out)
	return out
}

func (f *fakeValues) harvestedAuth() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.harvested...)
}

// guardValues 构造账号 42 被接管、且持有指定模型 state 的取值源。
func guardValues(states map[string]string) *fakeValues {
	return &fakeValues{
		enabled: map[int64]bool{42: true},
		values:  map[int64]map[string]string{42: states},
	}
}

func mustConfig(t *testing.T, raw string) pluginconfig.Config {
	t.Helper()
	if raw == "" {
		raw = "{}"
	}
	config, err := pluginconfig.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("解析测试配置失败: %v", err)
	}
	return config
}

func guardConfig(t *testing.T, overrideMode, onUnavailable string) pluginconfig.Config {
	t.Helper()
	return mustConfig(t, `{"overload_guard":{"override_mode":"`+overrideMode+`","on_unavailable":"`+onUnavailable+`",
		"accounts":[{"account_id":42,"enabled":true}]}}`)
}

// guardConfigUnmatched 只改「模型对不上时怎么办」，其余取默认值。
func guardConfigUnmatched(t *testing.T, onModelUnmatched string) pluginconfig.Config {
	t.Helper()
	return mustConfig(t, `{"overload_guard":{"on_model_unmatched":"`+onModelUnmatched+`",
		"accounts":[{"account_id":42,"enabled":true}]}}`)
}

func TestInjectAlwaysOverwritesClientValue(t *testing.T) {
	forwarder := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough),
		guardValues(map[string]string{"gpt-5-codex": "from-pool"}))
	defer forwarder.Close()

	// 客户端回带的是小写键名，注入必须替换它而不是并存两份。
	header := http.Header{"x-codex-turn-state": []string{"from-client"}, "User-Agent": []string{"codex"}}
	if err := forwarder.Inject(header, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("Inject 出错: %v", err)
	}
	if got := header.Get(pluginconfig.DefaultHeaderName); got != "from-pool" {
		t.Fatalf("注入结果 = %q，期望票池里的值", got)
	}
	var count int
	for name := range header {
		if strings.EqualFold(name, pluginconfig.DefaultHeaderName) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("过载防护头出现了 %d 次，期望 1 次", count)
	}
	if header.Get("User-Agent") != "codex" {
		t.Fatal("其他请求头不应被改动")
	}
}

// 每个模型注入自己的 state，绝不互相串用。
func TestInjectMatchesStateToModel(t *testing.T) {
	forwarder := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough),
		guardValues(map[string]string{"gpt-5-codex": "codex-state", "gpt-5": "five-state"}))
	defer forwarder.Close()

	for model, want := range map[string]string{"gpt-5-codex": "codex-state", "gpt-5": "five-state"} {
		header := http.Header{}
		if err := forwarder.Inject(header, 42, model); err != nil {
			t.Fatalf("模型 %s 注入出错: %v", model, err)
		}
		if got := header.Get(pluginconfig.DefaultHeaderName); got != want {
			t.Fatalf("模型 %s 注入了 %q，期望 %q", model, got, want)
		}
	}
}

func TestInjectFillMissingKeepsClientValue(t *testing.T) {
	forwarder := New(guardConfig(t, pluginconfig.OverrideModeFillMissing, pluginconfig.OnUnavailablePassthrough),
		guardValues(map[string]string{"gpt-5-codex": "from-pool"}))
	defer forwarder.Close()

	header := http.Header{"x-codex-turn-state": []string{"from-client"}}
	if err := forwarder.Inject(header, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("Inject 出错: %v", err)
	}
	// 客户端原本的键名大小写被保留，所以这里按大小写无关的方式取值。
	if got, _ := headerLookup(header, pluginconfig.DefaultHeaderName); got != "from-client" {
		t.Fatalf("fill_missing 不应覆盖客户端值，得到 %q", got)
	}
	if len(header) != 1 {
		t.Fatalf("fill_missing 不应额外写入一份同名头: %v", header)
	}

	empty := http.Header{}
	if err := forwarder.Inject(empty, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("Inject 出错: %v", err)
	}
	if got := empty.Get(pluginconfig.DefaultHeaderName); got != "from-pool" {
		t.Fatalf("缺失时应当写入票池里的值，得到 %q", got)
	}
}

func TestInjectValueUnavailable(t *testing.T) {
	values := &fakeValues{enabled: map[int64]bool{42: true}}

	passthrough := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough), values)
	defer passthrough.Close()
	header := http.Header{"User-Agent": []string{"codex"}}
	if err := passthrough.Inject(header, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("passthrough 不应报错: %v", err)
	}
	if header.Get(pluginconfig.DefaultHeaderName) != "" {
		t.Fatal("没有可用值时不应注入")
	}

	failing := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailableFailRequest), values)
	defer failing.Close()
	err := failing.Inject(http.Header{}, 42, "gpt-5-codex")
	if err == nil {
		t.Fatal("fail_request 应当返回错误")
	}
	if err.Code != CodeValueUnavailable || err.RequestSent {
		t.Fatalf("错误语义不正确: %+v", err)
	}
}

// 「一个 state 都还没拉到」与「拉到了但没有这个模型的」是两类问题：
// 前者走 on_unavailable，后者走 on_model_unmatched，且都不会借用别的模型的 state。
func TestInjectModelUnmatched(t *testing.T) {
	values := guardValues(map[string]string{"gpt-5-codex": "codex-state"})

	passthrough := New(guardConfigUnmatched(t, pluginconfig.OnUnavailablePassthrough), values)
	defer passthrough.Close()
	header := http.Header{"x-codex-turn-state": []string{"from-client"}}
	if err := passthrough.Inject(header, 42, "gpt-5"); err != nil {
		t.Fatalf("passthrough 不应报错: %v", err)
	}
	if got, _ := headerLookup(header, pluginconfig.DefaultHeaderName); got != "from-client" {
		t.Fatalf("模型对不上时应当原样透传，得到 %q", got)
	}

	failing := New(guardConfigUnmatched(t, pluginconfig.OnUnavailableFailRequest), values)
	defer failing.Close()
	err := failing.Inject(http.Header{}, 42, "gpt-5")
	if err == nil {
		t.Fatal("fail_request 应当返回错误")
	}
	if err.Code != CodeModelUnmatched || err.RequestSent {
		t.Fatalf("错误语义不正确: %+v", err)
	}
	if !strings.Contains(err.Message, "gpt-5") || !strings.Contains(err.Message, "跨模型") {
		t.Fatalf("错误信息应当点明是哪个模型、为什么不能复用: %q", err.Message)
	}
	// 与「完全没有值」区分开：错误码不同，排错时能直接定位。
	if err.Code == CodeValueUnavailable {
		t.Fatal("模型对不上不应报成 VALUE_UNAVAILABLE")
	}
}

// 请求体里解析不出模型名时同样按 on_model_unmatched 处理：不知道该用哪个 state，就不猜。
func TestInjectUnknownModelFollowsUnmatchedPolicy(t *testing.T) {
	values := guardValues(map[string]string{"gpt-5-codex": "codex-state"})

	passthrough := New(guardConfigUnmatched(t, pluginconfig.OnUnavailablePassthrough), values)
	defer passthrough.Close()
	header := http.Header{}
	if err := passthrough.Inject(header, 42, ""); err != nil {
		t.Fatalf("passthrough 不应报错: %v", err)
	}
	if len(header) != 0 {
		t.Fatalf("模型未知时不应注入任何 state: %v", header)
	}

	failing := New(guardConfigUnmatched(t, pluginconfig.OnUnavailableFailRequest), values)
	defer failing.Close()
	err := failing.Inject(http.Header{}, 42, "")
	if err == nil || err.Code != CodeModelUnmatched || err.RequestSent {
		t.Fatalf("错误语义不正确: %+v", err)
	}
	if !strings.Contains(err.Message, "解析出模型名") {
		t.Fatalf("错误信息应当说明是解析不出模型名: %q", err.Message)
	}
}

func TestInjectSkipsAccountsOutsideGuard(t *testing.T) {
	config := guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailableFailRequest)
	forwarder := New(config, guardValues(map[string]string{"gpt-5-codex": "v"}))
	defer forwarder.Close()

	header := http.Header{}
	// 账号 99 不在过载防护名单里：既不注入，也不能因为 fail_request 而拒绝请求。
	if err := forwarder.Inject(header, 99, "gpt-5-codex"); err != nil {
		t.Fatalf("未接管的账号不应报错: %v", err)
	}
	if len(header) != 0 {
		t.Fatalf("未接管的账号不应被注入: %v", header)
	}
}

func TestInjectAppliesExtraHeaders(t *testing.T) {
	config := mustConfig(t, `{"extra_headers":{"X-Trace":"plugin"},"overload_guard":{"enabled":false}}`)
	forwarder := New(config, nil)
	defer forwarder.Close()

	header := http.Header{"x-trace": []string{"client"}}
	if err := forwarder.Inject(header, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("Inject 出错: %v", err)
	}
	if got := header.Get("X-Trace"); got != "plugin" {
		t.Fatalf("extra_headers 未覆盖同名头: %q", got)
	}
	if len(header) != 1 {
		t.Fatalf("同名头应当只剩一份: %v", header)
	}
}

// 总开关关闭时，即便账号在名单里也不注入。
func TestInjectRespectsMasterSwitch(t *testing.T) {
	config := guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailableFailRequest)
	config.OverloadGuard.Enabled = false
	forwarder := New(config, guardValues(map[string]string{"gpt-5-codex": "state-a"}))
	defer forwarder.Close()

	header := http.Header{}
	if err := forwarder.Inject(header, 42, "gpt-5-codex"); err != nil {
		t.Fatalf("总开关关闭时不应报错: %v", err)
	}
	if len(header) != 0 {
		t.Fatalf("总开关关闭时不应注入: %v", header)
	}
}

func TestDoInjectsHeaderAndPreservesContentLength(t *testing.T) {
	type received struct {
		header           http.Header
		body             string
		contentLength    int64
		transferEncoding []string
		host             string
	}
	var got received
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		got = received{
			header:           request.Header.Clone(),
			body:             string(body),
			contentLength:    request.ContentLength,
			transferEncoding: request.TransferEncoding,
			host:             request.Host,
		}
		writer.Header().Set("X-Upstream", "ok")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	values := guardValues(map[string]string{"gpt-5-codex": "state-a", "gpt-5": "state-b"})
	forwarder := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough), values)
	defer forwarder.Close()

	payload := `{"stream":true,"model":"gpt-5-codex","input":[]}`
	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/responses",
		Host:          "chatgpt.com",
		Header:        http.Header{"Content-Type": []string{"application/json"}, "Content-Length": []string{"999"}},
		AccountID:     42,
		ContentLength: int64(len(payload)),
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader(payload)),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated || response.Header.Get("X-Upstream") != "ok" {
		t.Fatalf("响应未原样透传: %d %v", response.StatusCode, response.Header)
	}
	responseBody, _ := io.ReadAll(response.Body)
	if string(responseBody) != `{"ok":true}` {
		t.Fatalf("响应体 = %q", responseBody)
	}
	// 嗅探模型名之后请求体必须逐字节原样重放。
	if got.body != payload {
		t.Fatalf("上游收到的请求体 = %q", got.body)
	}
	if got.contentLength != int64(len(payload)) {
		t.Fatalf("上游 ContentLength = %d，期望 %d（不能被改成 chunked）", got.contentLength, len(payload))
	}
	if len(got.transferEncoding) != 0 {
		t.Fatalf("上游 TransferEncoding = %v，期望为空", got.transferEncoding)
	}
	// 注入的是请求体里那个模型的 state，不是另一个模型的。
	if value := got.header.Get(pluginconfig.DefaultHeaderName); value != "state-a" {
		t.Fatalf("上游收到的过载防护头 = %q，期望 gpt-5-codex 的 state", value)
	}
	if got.host != "chatgpt.com" {
		t.Fatalf("Host = %q，期望使用 start 帧给出的 Host", got.host)
	}
	if values := got.header.Values("Content-Length"); len(values) > 1 {
		t.Fatalf("Content-Length 出现了多份: %v", values)
	}
	if observed := values.observedModels(); len(observed) != 1 || observed[0] != "gpt-5-codex" {
		t.Fatalf("实际使用的模型应当被记录下来供下轮拉取: %v", observed)
	}
}

// 票池里没有这个模型的票时（passthrough），请求照常发出但不带头，请求体也不能被吃掉。
func TestDoPassesThroughWhenModelHasNoState(t *testing.T) {
	var gotHeader string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotHeader = request.Header.Get(pluginconfig.DefaultHeaderName)
		body, _ := io.ReadAll(request.Body)
		gotBody = string(body)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	values := guardValues(map[string]string{"gpt-5-codex": "codex-state"})
	forwarder := New(guardConfigUnmatched(t, pluginconfig.OnUnavailablePassthrough), values)
	defer forwarder.Close()

	payload := `{"model":"gpt-5","input":[]}`
	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/responses",
		Header:        http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		AccountID:     42,
		ContentLength: int64(len(payload)),
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader(payload)),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if gotHeader != "" {
		t.Fatalf("模型没有 state 时不应注入任何值，上游收到 %q", gotHeader)
	}
	if gotBody != payload {
		t.Fatalf("上游收到的请求体 = %q，期望原样重放", gotBody)
	}
	if observed := values.observedModels(); len(observed) != 1 || observed[0] != "gpt-5" {
		t.Fatalf("即便没有 state，也应记录模型以便下轮拉取: %v", observed)
	}
}

// 非 JSON 的出站请求不嗅探请求体：那里不会有模型名，缓冲它只是浪费。
func TestDoDoesNotSniffNonJSONBody(t *testing.T) {
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		gotBody = string(body)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	values := guardValues(map[string]string{"gpt-5-codex": "codex-state"})
	forwarder := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough), values)
	defer forwarder.Close()

	payload := `{"model":"gpt-5-codex"}`
	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/audio",
		Header:        http.Header{"Content-Type": []string{"application/octet-stream"}},
		AccountID:     42,
		ContentLength: int64(len(payload)),
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader(payload)),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if gotBody != payload {
		t.Fatalf("上游收到的请求体 = %q", gotBody)
	}
	if observed := values.observedModels(); len(observed) != 0 {
		t.Fatalf("非 JSON 请求体不应产生模型观测: %v", observed)
	}
}

func TestDoWithoutBody(t *testing.T) {
	var contentLength int64 = -1
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		contentLength = request.ContentLength
		body, _ := io.ReadAll(request.Body)
		if len(body) != 0 {
			t.Errorf("上游不应收到请求体，收到 %d 字节", len(body))
		}
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodGet,
		URL:           server.URL + "/v1/models",
		Header:        http.Header{},
		AccountID:     42,
		ContentLength: 0,
		HasBody:       false,
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	if contentLength != 0 {
		t.Fatalf("无体请求的 ContentLength = %d，期望 0", contentLength)
	}
}

// 请求体长度未知（content_length < 0）时才允许走 chunked。
func TestDoWithUnknownContentLength(t *testing.T) {
	var transferEncoding []string
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		transferEncoding = append([]string(nil), request.TransferEncoding...)
		data, _ := io.ReadAll(request.Body)
		body = string(data)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/responses",
		Header:        http.Header{},
		AccountID:     42,
		ContentLength: -1,
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader("streamed")),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	if body != "streamed" {
		t.Fatalf("上游收到的请求体 = %q", body)
	}
	if len(transferEncoding) != 1 || transferEncoding[0] != "chunked" {
		t.Fatalf("长度未知时应当走 chunked，得到 %v", transferEncoding)
	}
}

// 宿主侧 Request.ContentLength 为 0 而 Body 非空表示「长度未知」，不是空请求体：
// 必须照样把请求体流给上游（chunked），不能被当成 NoBody 丢掉。
func TestDoWithZeroContentLengthBodyStaysChunked(t *testing.T) {
	var transferEncoding []string
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		transferEncoding = append([]string(nil), request.TransferEncoding...)
		data, _ := io.ReadAll(request.Body)
		body = string(data)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/responses",
		Header:        http.Header{},
		AccountID:     42,
		ContentLength: 0,
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader("streamed")),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	if body != "streamed" {
		t.Fatalf("上游收到的请求体 = %q，长度为 0 的非空请求体被丢掉了", body)
	}
	if len(transferEncoding) != 1 || transferEncoding[0] != "chunked" {
		t.Fatalf("长度未知时应当走 chunked，得到 %v", transferEncoding)
	}
}

// 嗅探过模型名的流式请求体（长度未知）仍要保持 chunked 与逐字节一致。
func TestDoSniffedBodyKeepsChunkedStreaming(t *testing.T) {
	var transferEncoding []string
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		transferEncoding = append([]string(nil), request.TransferEncoding...)
		data, _ := io.ReadAll(request.Body)
		body = string(data)
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	forwarder := New(guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough),
		guardValues(map[string]string{"gpt-5-codex": "state-a"}))
	defer forwarder.Close()

	payload := `{"model":"gpt-5-codex","input":[{"role":"user","content":"` + strings.Repeat("字", 5000) + `"}]}`
	response, err := forwarder.Do(context.Background(), Request{
		Method:        http.MethodPost,
		URL:           server.URL + "/v1/responses",
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		AccountID:     42,
		ContentLength: -1,
		HasBody:       true,
		Body:          io.NopCloser(strings.NewReader(payload)),
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if body != payload {
		t.Fatalf("上游收到的请求体长度 = %d，期望 %d 且逐字节一致", len(body), len(payload))
	}
	if len(transferEncoding) != 1 || transferEncoding[0] != "chunked" {
		t.Fatalf("长度未知时应当仍走 chunked，得到 %v", transferEncoding)
	}
}

func TestDoStreamsResponseIncrementally(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		flusher, ok := writer.(http.Flusher)
		if !ok {
			t.Error("httptest 响应不支持 Flush")
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("data: first\n\n"))
		flusher.Flush()
		<-release
		_, _ = writer.Write([]byte("data: second\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method: http.MethodPost, URL: server.URL, Header: http.Header{}, AccountID: 42,
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()

	buffer := make([]byte, 64)
	read, readErr := response.Body.Read(buffer)
	if readErr != nil {
		t.Fatalf("读取首个分块失败: %v", readErr)
	}
	if !strings.Contains(string(buffer[:read]), "first") {
		t.Fatalf("首个分块 = %q，期望在上游 Flush 后立即可读", buffer[:read])
	}
	close(release)
	rest, err2 := io.ReadAll(response.Body)
	if err2 != nil {
		t.Fatalf("读取剩余响应失败: %v", err2)
	}
	if !strings.Contains(string(rest), "second") {
		t.Fatalf("剩余响应 = %q", rest)
	}
}

func TestDoPassesThroughUpstreamErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Retry-After", "30")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method: http.MethodPost, URL: server.URL, Header: http.Header{}, AccountID: 42,
	})
	if err != nil {
		t.Fatalf("上游 4xx 不是插件错误，应当原样返回: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests || response.Header.Get("Retry-After") != "30" {
		t.Fatalf("上游错误响应未原样透传: %d %v", response.StatusCode, response.Header)
	}
}

func TestDoDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirected" {
			t.Error("插件不应跟随重定向")
		}
		writer.Header().Set("Location", "/redirected")
		writer.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	response, err := forwarder.Do(context.Background(), Request{
		Method: http.MethodGet, URL: server.URL, Header: http.Header{}, AccountID: 42,
	})
	if err != nil {
		t.Fatalf("Do 出错: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound || response.Header.Get("Location") != "/redirected" {
		t.Fatalf("重定向响应应当原样返回: %d", response.StatusCode)
	}
}

func TestDoUpstreamFailureMarksRequestSent(t *testing.T) {
	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	_, err := forwarder.Do(context.Background(), Request{
		Method: http.MethodPost, URL: "http://127.0.0.1:1/v1/responses", Header: http.Header{}, AccountID: 42,
	})
	if err == nil {
		t.Fatal("连接失败应当报错")
	}
	if err.Code != CodeUpstreamError || !err.RequestSent {
		t.Fatalf("进入 RoundTrip 之后的失败必须标记 request_sent=true: %+v", err)
	}
	if strings.ContainsAny(err.Message, "\r\n") {
		t.Fatalf("错误信息不应包含换行: %q", err.Message)
	}
}

func TestDoInvalidProxyIsNotSent(t *testing.T) {
	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	_, err := forwarder.Do(context.Background(), Request{
		Method:    http.MethodPost,
		URL:       "https://chatgpt.com/backend-api/codex/responses",
		Header:    http.Header{},
		AccountID: 42,
		ProxyURL:  "ftp://user:password@proxy.example.com:1080",
	})
	if err == nil {
		t.Fatal("非法代理应当报错")
	}
	if err.Code != CodeConfigError || err.RequestSent {
		t.Fatalf("代理配置错误发生在发出请求之前: %+v", err)
	}
	if strings.Contains(err.Message, "password") {
		t.Fatalf("错误信息泄露了代理凭据: %q", err.Message)
	}
}

func TestDoRespectsContextCancellation(t *testing.T) {
	blocked := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(blocked)
	}))
	defer server.Close()

	forwarder := New(mustConfig(t, ""), nil)
	defer forwarder.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := forwarder.Do(ctx, Request{
		Method: http.MethodPost, URL: server.URL, Header: http.Header{}, AccountID: 42,
	})
	if err == nil {
		t.Fatal("context 取消后应当报错")
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("上游请求未随 context 取消")
	}
}

// 请求被拒绝时，仍要把宿主正在发送的请求体读完，否则宿主会卡住。
// 嗅探过的请求体（replayBody）也必须被同样处理。
func TestDoDrainsBodyWhenRejected(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		payload     string
		values      *fakeValues
		config      pluginconfig.Config
		wantCode    string
	}{
		{
			name:        "还没拉到任何 state",
			contentType: "text/plain",
			payload:     "payload",
			values:      &fakeValues{enabled: map[int64]bool{42: true}},
			config:      guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailableFailRequest),
			wantCode:    CodeValueUnavailable,
		},
		{
			name:        "嗅探过请求体但没有该模型的 state",
			contentType: "application/json",
			payload:     `{"model":"gpt-5","input":[]}`,
			values:      guardValues(map[string]string{"gpt-5-codex": "codex-state"}),
			config:      guardConfigUnmatched(t, pluginconfig.OnUnavailableFailRequest),
			wantCode:    CodeModelUnmatched,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forwarder := New(tc.config, tc.values)
			defer forwarder.Close()

			body := &trackingBody{reader: strings.NewReader(tc.payload)}
			_, err := forwarder.Do(context.Background(), Request{
				Method: http.MethodPost, URL: "https://chatgpt.com/",
				Header:    http.Header{"Content-Type": []string{tc.contentType}},
				AccountID: 42, ContentLength: int64(len(tc.payload)), HasBody: true, Body: body,
			})
			if err == nil || err.Code != tc.wantCode {
				t.Fatalf("应当以 %s 拒绝: %+v", tc.wantCode, err)
			}
			if err.RequestSent {
				t.Fatalf("尚未调用上游 Transport，request_sent 必须为 false: %+v", err)
			}
			if !body.closed {
				t.Fatal("被拒绝的请求体应当被关闭")
			}
			if body.remaining() != 0 {
				t.Fatal("被拒绝的请求体应当被读空")
			}
		})
	}
}

type trackingBody struct {
	reader *strings.Reader
	closed bool
}

func (b *trackingBody) Read(data []byte) (int, error) { return b.reader.Read(data) }

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

func (b *trackingBody) remaining() int { return b.reader.Len() }

// 撞票用的 OAuth 凭据只能从过路请求里取，所以采集必须发生在注入之前——
// 否则读到的会是插件自己写进去的头；未接管的账号则完全不采集。
func TestDoHarvestsCredentialBeforeInjection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := guardConfig(t, pluginconfig.OverrideModeAlways, pluginconfig.OnUnavailablePassthrough)
	// extra_headers 会在注入阶段改写 Authorization：采集若晚一步就会取到这个假值。
	config.ExtraHeaders = map[string]string{"Authorization": "Bearer injected-by-plugin"}
	values := guardValues(map[string]string{"gpt-5-codex": "state-a"})
	forwarder := New(config, values)
	defer forwarder.Close()

	call := func(accountID int64) {
		payload := `{"model":"gpt-5-codex"}`
		response, err := forwarder.Do(context.Background(), Request{
			Method: http.MethodPost, URL: server.URL + "/responses",
			Header: http.Header{
				"Content-Type":  []string{"application/json"},
				"Authorization": []string{"Bearer real-client-token"},
			},
			AccountID: accountID, ContentLength: int64(len(payload)), HasBody: true,
			Body: io.NopCloser(strings.NewReader(payload)),
		})
		if err != nil {
			t.Fatalf("账号 %d 的 Do 出错: %v", accountID, err)
		}
		_ = response.Body.Close()
	}

	call(42)
	call(99)

	harvested := values.harvestedAuth()
	if len(harvested) != 1 {
		t.Fatalf("只应采集被接管账号的凭据，实际采集 %d 次: %v", len(harvested), harvested)
	}
	if harvested[0] != "Bearer real-client-token" {
		t.Fatalf("采集到的应当是客户端原始凭据，得到 %q", harvested[0])
	}
}
