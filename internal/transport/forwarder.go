package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/pluginconfig"
)

// 错误码会原样出现在宿主日志与错误信息里，保持稳定、可检索。
const (
	CodeConfigError      = "OVERLOAD_GUARD_CONFIG_ERROR"
	CodeValueUnavailable = "OVERLOAD_GUARD_VALUE_UNAVAILABLE"
	CodeModelUnmatched   = "OVERLOAD_GUARD_MODEL_UNMATCHED"
	CodeUpstreamError    = "OVERLOAD_GUARD_UPSTREAM_ERROR"
)

// Error 是出站失败的结构化结果，字段直接映射到 ForwardResponseError。
//
// RequestSent 必须准确：只有在能够确认尚未调用上游 Transport 时才是 false，
// 宿主据此判断能否换账号重放（docs/PLUGIN_DEVELOPMENT.md）。
type Error struct {
	Code        string
	Message     string
	RequestSent bool
}

func (e *Error) Error() string {
	if e == nil {
		return "出站请求失败"
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// ValueSource 提供账号当前可注入的过载防护值，由 internal/ticketpool 的票池管理器实现。
//
// state 头按模型区分，所以取值必须带上模型名；Ready 用来把「还没撞到任何 state」
// 与「撞到了但没有这个模型的 state」分开，两者对应不同的策略配置。
type ValueSource interface {
	// Enabled 表示账号是否被过载防护接管。
	Enabled(accountID int64) bool
	// Ready 表示账号已有一批仍在有效期内的 state（不保证含某个具体模型）。
	Ready(accountID int64) bool
	// Value 返回该模型当前可用的 state；false 表示这个模型没有可注入的值。
	Value(accountID int64, model string) (string, bool)
	// Observe 记录账号实际用到的模型，供票池为它单独建池。
	Observe(accountID int64, model string)
	// Harvest 从过路请求里取走撞票所需的账号凭据（Authorization 等）。
	//
	// 插件不依赖任何外部服务端，账号的 OAuth access token 只能从真实请求里获得：
	// 实现方必须只留在内存里，不落盘、不进日志、不出现在健康检查与测试输出中。
	Harvest(accountID int64, header http.Header, rawURL string)
}

// Request 是从 Forward 的 start 帧解析出的出站请求描述。
type Request struct {
	Method        string
	URL           string
	Host          string
	Header        http.Header
	ProxyURL      string
	AccountID     int64
	ContentLength int64
	HasBody       bool
	Body          io.ReadCloser
}

// Forwarder 用一份已生效的配置发出上游请求。它是只读的，可被并发调用。
type Forwarder struct {
	config pluginconfig.Config
	pool   *Pool
	values ValueSource
}

// New 创建 Forwarder。config 必须已通过 pluginconfig 校验。
func New(config pluginconfig.Config, values ValueSource) *Forwarder {
	return &Forwarder{config: config, pool: NewPool(config), values: values}
}

// Close 释放连接池。配置切换时在新 Forwarder 生效之后调用。
func (f *Forwarder) Close() {
	if f != nil && f.pool != nil {
		f.pool.Close()
	}
}

// Pool 暴露连接池，供状态诊断复用同一套连接参数与代理实现。
func (f *Forwarder) Pool() *Pool {
	return f.pool
}

// RequestTimeout 返回配置的整体超时；0 表示不限制（流式响应的推荐值）。
func (f *Forwarder) RequestTimeout() (int, bool) {
	if f.config.RequestTimeoutSeconds <= 0 {
		return 0, false
	}
	return f.config.RequestTimeoutSeconds, true
}

// Do 注入请求头并发出上游请求。
//
// 返回错误时 Error.RequestSent 已经按语义填好：注入失败与客户端构建失败都发生在
// 调用上游 Transport 之前（false），RoundTrip 本身失败则无法确认上游是否收到（true）。
// 调用方负责关闭返回的响应体。
func (f *Forwarder) Do(ctx context.Context, request Request) (*http.Response, *Error) {
	header := cloneHeader(request.Header)
	guarded := f.guarded(request.AccountID)

	// 撞票用的 OAuth 凭据只能从真实请求里拿：先取凭据，再做任何注入，
	// 否则拿到的会是插件自己写进去的头。未接管的账号一律不采集。
	if guarded {
		f.values.Harvest(request.AccountID, header, request.URL)
	}

	// 只有账号确实被接管时才嗅探模型名：纯透传的账号不该为此缓冲请求体。
	model := ""
	if guarded && request.HasBody && request.Body != nil && isJSONBody(header) {
		model, request.Body = peekModel(request.Body, f.config.OverloadGuard.SniffLimit())
		if model != "" {
			f.values.Observe(request.AccountID, model)
		}
	}

	if err := f.Inject(header, request.AccountID, model); err != nil {
		f.closeBody(request.Body)
		return nil, err
	}

	client, clientErr := f.pool.Client(request.ProxyURL)
	if clientErr != nil {
		f.closeBody(request.Body)
		return nil, &Error{Code: CodeConfigError, Message: clientErr.Error(), RequestSent: false}
	}

	httpRequest, buildErr := f.buildRequest(ctx, request, header)
	if buildErr != nil {
		f.closeBody(request.Body)
		return nil, &Error{Code: CodeConfigError, Message: buildErr.Error(), RequestSent: false}
	}

	response, err := client.Do(httpRequest)
	if err != nil {
		// 已经进入 RoundTrip：无法证明上游没收到请求，必须禁止宿主换号重放。
		return nil, &Error{
			Code:        CodeUpstreamError,
			Message:     SanitizeMessage(err.Error()),
			RequestSent: true,
		}
	}
	return response, nil
}

// guarded 判断账号是否被过载防护接管。
func (f *Forwarder) guarded(accountID int64) bool {
	return f.config.OverloadGuard.Enabled && f.values != nil && f.values.Enabled(accountID)
}

// Inject 把插件的额外头与过载防护头写入 header。
//
// 规则：
//   - extra_headers 始终生效（配置层已禁止覆盖宿主受保护头）；
//   - 账号未被接管 → 完全透传，不注入；
//   - override_mode=always → 覆盖客户端回带的值；fill_missing → 仅在缺失时写入；
//   - 该模型没有 state（含解析不出模型名）→ on_model_unmatched 决定透传还是拒绝；
//   - 一个 state 都还没拉到 → on_unavailable 决定透传还是拒绝。
//
// 注意这里绝不会在模型之间借用 state：上游按模型铸造该 blob，串了模型比不带头更糟。
func (f *Forwarder) Inject(header http.Header, accountID int64, model string) *Error {
	for name, value := range f.config.ExtraHeaders {
		headerAssign(header, name, value)
	}

	guard := f.config.OverloadGuard
	if !guard.Enabled || f.values == nil || !f.values.Enabled(accountID) {
		return nil
	}

	value, ok := f.values.Value(accountID, model)
	if ok {
		if guard.OverrideMode == pluginconfig.OverrideModeFillMissing {
			if existing, present := headerLookup(header, guard.HeaderName); present && existing != "" {
				return nil
			}
		}
		headerAssign(header, guard.HeaderName, value)
		return nil
	}

	// 有值但不含该模型，和完全没有值，是两类问题，策略与提示都不同。
	if f.values.Ready(accountID) {
		if guard.OnModelUnmatched == pluginconfig.OnUnavailableFailRequest {
			return &Error{
				Code:        CodeModelUnmatched,
				Message:     modelUnmatchedMessage(accountID, model, guard.HeaderName),
				RequestSent: false,
			}
		}
		// passthrough：不注入，原样转发。
		return nil
	}

	if guard.OnUnavailable == pluginconfig.OnUnavailableFailRequest {
		return &Error{
			Code: CodeValueUnavailable,
			Message: fmt.Sprintf("账号 %d 暂时没有可用的过载防护值（%s），已按 fail_request 策略拒绝请求",
				accountID, guard.HeaderName),
			RequestSent: false,
		}
	}
	// passthrough：不注入，原样转发，让请求照常打到上游。
	return nil
}

func modelUnmatchedMessage(accountID int64, model, headerName string) string {
	if model == "" {
		return fmt.Sprintf("账号 %d 无法从请求体中解析出模型名，%s 按模型区分、无法确定该用哪个值，"+
			"已按 fail_request 策略拒绝请求", accountID, headerName)
	}
	return fmt.Sprintf("账号 %d 的票池里没有模型 %s 的 %s（state 按模型铸造，不能跨模型复用），"+
		"已按 fail_request 策略拒绝请求", accountID, model, headerName)
}

func (f *Forwarder) buildRequest(ctx context.Context, request Request, header http.Header) (*http.Request, error) {
	method := request.Method
	if method == "" {
		method = http.MethodGet
	}

	body := request.Body
	if !request.HasBody || body == nil {
		body = nil
	}

	httpRequest, err := http.NewRequestWithContext(ctx, method, request.URL, body)
	if err != nil {
		return nil, fmt.Errorf("构造上游请求失败: %w", err)
	}
	if body == nil {
		httpRequest.Body = http.NoBody
		httpRequest.ContentLength = 0
	} else {
		// 保留宿主给出的长度：content_length > 0 时必须仍以 Content-Length 发出，
		// 不能被改写成 chunked；<= 0 表示长度未知（宿主侧 Request.ContentLength 为 0
		// 且 Body 非空就是这种情况），交给 net/http 走 chunked，绝不能当成空请求体。
		httpRequest.ContentLength = request.ContentLength
	}

	httpRequest.Header = header
	if request.Host != "" {
		httpRequest.Host = request.Host
	}
	return httpRequest, nil
}

func (f *Forwarder) closeBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	// 请求未发出时也把宿主正在写入的请求体读完再关：宿主在收到 error 帧后会取消流，
	// 不会真的卡死，但读完能让拒绝路径与正常路径对请求体的处理保持一致，避免半截关闭。
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

// cloneHeader 复制头表，避免修改调用方（帧解析结果）持有的 map。
//
// 键名刻意保持宿主给出的原样大小写：宿主会精心构造上游请求头，规范化反而会改变
// 出站指纹。Content-Length 与 Transfer-Encoding 由 net/http 依据 ContentLength
// 字段自行生成，保留进来的副本会造成重复头。
func cloneHeader(header http.Header) http.Header {
	out := make(http.Header, len(header)+1)
	for name, values := range header {
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		out[name] = append([]string(nil), values...)
	}
	return out
}

// headerLookup 忽略大小写查头值。帧里的键名大小写由宿主决定，不能假定已规范化。
func headerLookup(header http.Header, name string) (string, bool) {
	for key, values := range header {
		if !strings.EqualFold(key, name) {
			continue
		}
		if len(values) == 0 {
			return "", true
		}
		return values[0], true
	}
	return "", false
}

// headerAssign 先删掉所有大小写变体再写入，避免同一个头被发出两份
// （例如客户端回带了小写的 x-codex-turn-state，而我们注入规范大小写）。
func headerAssign(header http.Header, name, value string) {
	for key := range header {
		if strings.EqualFold(key, name) {
			delete(header, key)
		}
	}
	header[name] = []string{value}
}

// SanitizeMessage 压掉换行与过长内容，保证错误信息可安全写进宿主日志。
func SanitizeMessage(message string) string {
	message = strings.Map(func(char rune) rune {
		if char == '\n' || char == '\r' || char == '\t' {
			return ' '
		}
		if char < 0x20 || char == 0x7f {
			return -1
		}
		return char
	}, message)
	message = strings.TrimSpace(message)
	if len(message) > 500 {
		message = message[:500] + "…"
	}
	if message == "" {
		return "上游请求失败"
	}
	return message
}
