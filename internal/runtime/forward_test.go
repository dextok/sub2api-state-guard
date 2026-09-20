package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/pluginconfig"
	"github.com/dextok/sub2api-plugin-overload-guard/internal/transport"
)

// fakeStream 模拟宿主侧的 Forward 双向流。
type fakeStream struct {
	grpc.ServerStream

	ctx      context.Context
	requests chan *pluginv1.ForwardRequest

	mu     sync.Mutex
	sent   []*pluginv1.ForwardResponse
	onSend func(*pluginv1.ForwardResponse) error
}

func newFakeStream(ctx context.Context) *fakeStream {
	return &fakeStream{ctx: ctx, requests: make(chan *pluginv1.ForwardRequest, 64)}
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) Recv() (*pluginv1.ForwardRequest, error) {
	select {
	case frame, ok := <-f.requests:
		if !ok {
			return nil, io.EOF
		}
		return frame, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeStream) Send(response *pluginv1.ForwardResponse) error {
	f.mu.Lock()
	hook := f.onSend
	f.sent = append(f.sent, response)
	f.mu.Unlock()
	if hook != nil {
		return hook(response)
	}
	return nil
}

func (f *fakeStream) frames() []*pluginv1.ForwardResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pluginv1.ForwardResponse(nil), f.sent...)
}

// push 依次投递 start、请求体分块与 body_end，模拟宿主的发送顺序。
func (f *fakeStream) push(start *pluginv1.ForwardRequestStart, body string, sendBodyEnd bool) {
	f.requests <- &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: start}}
	if body != "" {
		f.requests <- &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(body)}}
	}
	if sendBodyEnd {
		f.requests <- &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}}
	}
	close(f.requests)
}

func startFrame(method, url string, header map[string]string, contentLength int64, hasBody bool) *pluginv1.ForwardRequestStart {
	headers := make(map[string]*pluginv1.HeaderValues, len(header))
	for name, value := range header {
		headers[name] = &pluginv1.HeaderValues{Values: []string{value}}
	}
	return &pluginv1.ForwardRequestStart{
		RequestId:     "test-request",
		Method:        method,
		Url:           url,
		Host:          "chatgpt.com",
		Headers:       headers,
		AccountId:     testAccountID,
		Platform:      "openai",
		AccountType:   "oauth",
		ContentLength: contentLength,
		HasBody:       hasBody,
	}
}

// collect 把回送的帧拆成响应头、响应体、结束帧与错误帧。
func collect(t *testing.T, frames []*pluginv1.ForwardResponse) (*pluginv1.ForwardResponseStart, string, *pluginv1.ForwardResponseEnd, *pluginv1.ForwardResponseError) {
	t.Helper()
	var (
		start   *pluginv1.ForwardResponseStart
		body    strings.Builder
		end     *pluginv1.ForwardResponseEnd
		failure *pluginv1.ForwardResponseError
	)
	for index, frame := range frames {
		switch {
		case frame.GetStart() != nil:
			if index != 0 {
				t.Fatalf("start 帧必须是第一帧，实际在第 %d 帧", index)
			}
			start = frame.GetStart()
		case frame.GetBodyChunk() != nil:
			if start == nil {
				t.Fatal("body_chunk 不能出现在 start 之前")
			}
			body.Write(frame.GetBodyChunk())
		case frame.GetEnd() != nil:
			if index != len(frames)-1 {
				t.Fatal("end 帧必须是最后一帧")
			}
			end = frame.GetEnd()
		case frame.GetError() != nil:
			if index != len(frames)-1 {
				t.Fatal("error 帧必须是最后一帧")
			}
			failure = frame.GetError()
		}
	}
	return start, body.String(), end, failure
}

func TestForwardInjectsHeaderAndStreamsResponse(t *testing.T) {
	var upstreamHeader http.Header
	var upstreamHost string
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamHeader = request.Header.Clone()
		upstreamHost = request.Host
		_, _ = io.ReadAll(request.Body)
		writer.Header().Set("X-Upstream", "ok")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("hello"))
	}))
	defer upstream.Close()

	server, gateway := newGuardedServer(t)
	value := warmPool(t, server, gateway)

	// 票按模型分池，所以必须从请求体里读出 model 才会注入。
	payload := fmt.Sprintf(`{"stream":true,"model":%q,"input":[]}`, testModel)
	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses",
		map[string]string{"User-Agent": "codex", "Content-Type": "application/json"},
		int64(len(payload)), true), payload, true)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	start, body, end, failure := collect(t, stream.frames())
	if failure != nil {
		t.Fatalf("不应有错误帧: %+v", failure)
	}
	if start == nil || start.GetStatusCode() != http.StatusOK {
		t.Fatalf("响应头帧 = %+v", start)
	}
	if values := start.GetHeaders()["X-Upstream"]; values == nil || len(values.GetValues()) != 1 || values.GetValues()[0] != "ok" {
		t.Fatalf("上游响应头未透传: %v", start.GetHeaders())
	}
	if body != "hello" {
		t.Fatalf("响应体 = %q", body)
	}
	if end == nil || end.GetBytesReceived() != int64(len("hello")) {
		t.Fatalf("结束帧 = %+v", end)
	}
	if got := upstreamHeader.Get(pluginconfig.DefaultHeaderName); got != value {
		t.Fatalf("上游收到的过载防护头长度 = %d，期望注入插件自己撞到的那张票", len(got))
	}
	if upstreamHeader.Get("User-Agent") != "codex" {
		t.Fatal("宿主给的请求头应当原样透传")
	}
	if upstreamHost != "chatgpt.com" {
		t.Fatalf("上游 Host = %q", upstreamHost)
	}
}

// 插件不依赖任何外部服务端，撞票用的 OAuth 令牌只能从过路请求里采。
// 一个账号必须先有一个真实请求经过，它的票池才会开始预热。
func TestForwardHarvestsCredentialFromRealRequest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.ReadAll(request.Body)
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)
	if server.state().tickets.Status().Accounts[0].HasCredential {
		t.Fatal("还没有请求经过，不该有凭据")
	}

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/backend-api/codex/responses",
		map[string]string{"Authorization": testBearer, "ChatGpt-Account-Id": "acct-9"},
		0, false), "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	if _, _, _, failure := collect(t, stream.frames()); failure != nil {
		t.Fatalf("不应有错误帧: %+v", failure)
	}

	account := server.state().tickets.Status().Accounts[0]
	if !account.HasCredential {
		t.Fatal("过路请求带了 Bearer 令牌，应当已被采集")
	}
	// 采到的令牌只留在内存里：状态里只能有「有无」与采集时间。
	if account.CredentialSeconds < 0 {
		t.Fatalf("凭据采集时间 = %d", account.CredentialSeconds)
	}
}

// 未被接管的账号不采凭据：管理员没勾的号，它的令牌不该进插件内存。
func TestForwardSkipsHarvestForUnmanagedAccount(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.ReadAll(request.Body)
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)
	frame := startFrame(http.MethodPost, upstream.URL+"/backend-api/codex/responses",
		map[string]string{"Authorization": testBearer}, 0, false)
	frame.AccountId = 999

	stream := newFakeStream(context.Background())
	stream.push(frame, "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	if server.state().tickets.Status().Accounts[0].HasCredential {
		t.Fatal("别的账号的请求不该给被接管账号写入凭据")
	}
}

// 请求体里读不出模型（或该模型没有票）时，按 on_model_unmatched 决定放行还是拒绝，
// 绝不能退而复用别的模型的票。
func TestForwardModelUnmatchedPolicies(t *testing.T) {
	var injected string
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		injected = request.Header.Get(pluginconfig.DefaultHeaderName)
		_, _ = io.ReadAll(request.Body)
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cases := []struct {
		name    string
		policy  string
		payload string
	}{
		{name: "passthrough_未知模型", policy: "passthrough", payload: `{"stream":true}`},
		{name: "passthrough_其他模型", policy: "passthrough", payload: `{"model":"gpt-5"}`},
		{name: "fail_request_其他模型", policy: "fail_request", payload: `{"model":"gpt-5"}`},
		{name: "fail_request_未知模型", policy: "fail_request", payload: `{"stream":true}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			injected = ""
			server, gateway := newGuardedServer(t,
				fmt.Sprintf(`"on_model_unmatched": %q,`, testCase.policy))
			// 先撞到一张票：有票才会走「模型未匹配」这条分支，否则算「无可用值」。
			warmPool(t, server, gateway)

			stream := newFakeStream(context.Background())
			stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses",
				map[string]string{"Content-Type": "application/json"},
				int64(len(testCase.payload)), true), testCase.payload, true)
			if err := server.Forward(stream); err != nil {
				t.Fatalf("Forward 出错: %v", err)
			}
			start, _, end, failure := collect(t, stream.frames())

			if testCase.policy == "fail_request" {
				if failure == nil || failure.GetCode() != transport.CodeModelUnmatched {
					t.Fatalf("错误帧 = %+v", failure)
				}
				if failure.GetRequestSent() {
					t.Fatal("请求尚未发出，request_sent 必须为 false，宿主才能安全换号")
				}
				if injected != "" {
					t.Fatalf("被拒绝的请求不应到达上游，却带了头 %q", injected)
				}
				return
			}
			if failure != nil {
				t.Fatalf("passthrough 时请求应当照常发出: %+v", failure)
			}
			if start == nil || start.GetStatusCode() != http.StatusOK || end == nil {
				t.Fatalf("帧序列不完整: start=%+v end=%+v", start, end)
			}
			if injected != "" {
				t.Fatalf("模型没有对应票时不能注入任何值，却注入了 %q", injected)
			}
		})
	}
}

func TestForwardStreamsRequestBody(t *testing.T) {
	type received struct {
		body             string
		contentLength    int64
		transferEncoding []string
	}
	var got received
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, _ := io.ReadAll(request.Body)
		got = received{
			body:             string(data),
			contentLength:    request.ContentLength,
			transferEncoding: append([]string(nil), request.TransferEncoding...),
		}
		writer.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	server, gateway := newGuardedServer(t)
	warmPool(t, server, gateway)

	payload := fmt.Sprintf(`{"model":%q}`, testModel)
	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses",
		map[string]string{"Content-Type": "application/json"}, int64(len(payload)), true), payload, true)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	start, _, end, failure := collect(t, stream.frames())
	if failure != nil {
		t.Fatalf("不应有错误帧: %+v", failure)
	}
	if start == nil || start.GetStatusCode() != http.StatusCreated || end == nil {
		t.Fatalf("帧序列不完整: start=%+v end=%+v", start, end)
	}
	if got.body != payload {
		t.Fatalf("上游收到的请求体 = %q", got.body)
	}
	if got.contentLength != int64(len(payload)) {
		t.Fatalf("上游 ContentLength = %d，期望 %d（不能被改成 chunked）", got.contentLength, len(payload))
	}
	if len(got.transferEncoding) != 0 {
		t.Fatalf("上游 TransferEncoding = %v，期望为空", got.transferEncoding)
	}
}

// 宿主发完分块却没有 body_end（宿主侧读体失败）时，不能让上游把半截请求体当成完整请求。
func TestForwardTruncatedRequestBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.ReadAll(request.Body)
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 128, true), "half", false)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	_, _, _, failure := collect(t, stream.frames())
	if failure == nil {
		t.Fatal("请求体被截断时必须回错误帧")
	}
	if !failure.GetRequestSent() {
		t.Fatalf("请求已经进入上游，request_sent 必须为 true: %+v", failure)
	}
}

func TestForwardStreamsChunksIncrementally(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		flusher := writer.(http.Flusher)
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("data: first\n\n"))
		flusher.Flush()
		<-release
		_, _ = writer.Write([]byte("data: second\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)

	stream := newFakeStream(context.Background())
	// 第一个分块一送出就放行上游写第二块：只有真正流式转发才能走到这一步。
	stream.onSend = func(response *pluginv1.ForwardResponse) error {
		if strings.Contains(string(response.GetBodyChunk()), "first") {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		return nil
	}
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	frames := stream.frames()
	start, body, end, failure := collect(t, frames)
	if failure != nil {
		t.Fatalf("不应有错误帧: %+v", failure)
	}
	if start == nil || end == nil {
		t.Fatal("帧序列不完整")
	}
	if !strings.Contains(body, "first") || !strings.Contains(body, "second") {
		t.Fatalf("响应体 = %q", body)
	}
	chunks := 0
	for _, frame := range frames {
		if frame.GetBodyChunk() != nil {
			chunks++
		}
	}
	if chunks < 2 {
		t.Fatalf("分块数 = %d，期望至少 2 块（逐块转发而不是攒齐再发）", chunks)
	}
}

func TestForwardRejectsNonStartFirstFrame(t *testing.T) {
	server := newServer(t)
	stream := newFakeStream(context.Background())
	stream.requests <- &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte("x")}}
	close(stream.requests)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	_, _, _, failure := collect(t, stream.frames())
	if failure == nil || failure.GetCode() != CodeProtocolError {
		t.Fatalf("错误帧 = %+v", failure)
	}
	if failure.GetRequestSent() {
		t.Fatal("协议错误发生在调用上游之前，request_sent 必须为 false")
	}
}

func TestForwardWithoutStartFrameReturnsNil(t *testing.T) {
	server := newServer(t)
	stream := newFakeStream(context.Background())
	close(stream.requests)

	if err := server.Forward(stream); err != nil {
		t.Fatalf("流在 start 之前断开时不应返回错误: %v", err)
	}
	if len(stream.frames()) != 0 {
		t.Fatalf("不应回送任何帧: %v", stream.frames())
	}
}

// 账号还一张票都没有（刚接管、还没预热完）时，按 on_unavailable 决定放行还是拒绝。
func TestForwardValueUnavailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get(pluginconfig.DefaultHeaderName) != "" {
			t.Error("没有可用票时不应注入过载防护头")
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	t.Run("fail_request", func(t *testing.T) {
		server, _ := newGuardedServer(t, `"on_unavailable": "fail_request",`)

		stream := newFakeStream(context.Background())
		stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)
		if err := server.Forward(stream); err != nil {
			t.Fatalf("Forward 出错: %v", err)
		}
		_, _, _, failure := collect(t, stream.frames())
		if failure == nil || failure.GetCode() != transport.CodeValueUnavailable {
			t.Fatalf("错误帧 = %+v", failure)
		}
		if failure.GetRequestSent() {
			t.Fatal("请求尚未发出，request_sent 必须为 false，宿主才能安全换号")
		}
	})

	t.Run("passthrough", func(t *testing.T) {
		server, _ := newGuardedServer(t, `"on_unavailable": "passthrough",`)

		stream := newFakeStream(context.Background())
		stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)
		if err := server.Forward(stream); err != nil {
			t.Fatalf("Forward 出错: %v", err)
		}
		start, _, end, failure := collect(t, stream.frames())
		if failure != nil {
			t.Fatalf("passthrough 时请求应当照常发出: %+v", failure)
		}
		if start == nil || start.GetStatusCode() != http.StatusOK || end == nil {
			t.Fatalf("帧序列不完整: start=%+v end=%+v", start, end)
		}
	})
}

func TestForwardUpstreamFailure(t *testing.T) {
	server, _ := newGuardedServer(t)

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, "http://127.0.0.1:1/v1/responses", nil, 0, false), "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	_, _, _, failure := collect(t, stream.frames())
	if failure == nil || failure.GetCode() != transport.CodeUpstreamError {
		t.Fatalf("错误帧 = %+v", failure)
	}
	if !failure.GetRequestSent() {
		t.Fatal("已进入 RoundTrip 的失败必须标记 request_sent=true")
	}
}

func TestForwardUpstreamErrorStatusPassesThrough(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Retry-After", "30")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	start, body, end, failure := collect(t, stream.frames())
	if failure != nil {
		t.Fatalf("上游 429 不是插件错误: %+v", failure)
	}
	if start == nil || start.GetStatusCode() != http.StatusTooManyRequests {
		t.Fatalf("状态码未透传: %+v", start)
	}
	if values := start.GetHeaders()["Retry-After"]; values == nil || values.GetValues()[0] != "30" {
		t.Fatalf("Retry-After 未透传: %v", start.GetHeaders())
	}
	if body != `{"error":"rate limited"}` || end == nil {
		t.Fatalf("响应体 = %q，结束帧 = %+v", body, end)
	}
}

func TestForwardBeforeReady(t *testing.T) {
	server := New(testIdentity(), nil)
	server.Close()

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, "https://chatgpt.com/", nil, 0, false), "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	_, _, _, failure := collect(t, stream.frames())
	if failure == nil || failure.GetCode() != transport.CodeConfigError || failure.GetRequestSent() {
		t.Fatalf("错误帧 = %+v", failure)
	}
}

func TestForwardRespectsStreamContextCancel(t *testing.T) {
	blocked := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		close(blocked)
	}))
	defer upstream.Close()

	server, _ := newGuardedServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeStream(ctx)
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("上游请求未随流 context 取消")
	}
	_, _, _, failure := collect(t, stream.frames())
	if failure == nil {
		t.Fatal("取消时应当回错误帧")
	}
}

func TestForwardAppliesRequestTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		writer.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newServer(t)
	applyConfig(t, server, []byte(`{"request_timeout_seconds":60,"overload_guard":{"enabled":false}}`))
	if timeout, ok := server.state().forwarder.RequestTimeout(); !ok || timeout != 60 {
		t.Fatalf("整体超时 = %d/%v", timeout, ok)
	}

	stream := newFakeStream(context.Background())
	stream.push(startFrame(http.MethodPost, upstream.URL+"/v1/responses", nil, 0, false), "", true)
	if err := server.Forward(stream); err != nil {
		t.Fatalf("Forward 出错: %v", err)
	}
	start, _, end, failure := collect(t, stream.frames())
	if failure != nil {
		t.Fatalf("60 秒整体超时不该在这个请求上触发: %+v", failure)
	}
	if start == nil || end == nil {
		t.Fatal("帧序列不完整")
	}
}
