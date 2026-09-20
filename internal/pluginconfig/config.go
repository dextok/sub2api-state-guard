// Package pluginconfig 定义 Sub2api State Guard 插件的配置结构、默认值与严格校验。
//
// 宿主把配置当作不透明 JSON 加密保存，校验与规范化完全由插件负责：
// ValidateConfig 与 ApplyConfig 复用本包同一套逻辑，空对象会被规范化为全默认配置。
package pluginconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// 过载防护头的默认名称：Codex 的回合状态头。
const DefaultHeaderName = "X-Codex-Turn-State"

// 传输默认值对齐宿主 backend/internal/repository/http_upstream.go 的常量，
// 避免插件接管出站后连接行为与宿主原有路径出现落差。
const (
	defaultResponseHeaderTimeoutSeconds = 300
	defaultDialTimeoutSeconds           = 10
	defaultTLSHandshakeTimeoutSeconds   = 10
	defaultIdleConnectionTimeoutSeconds = 90
	defaultMaxIdleConnections           = 240
	defaultMaxIdleConnectionsPerHost    = 120
	defaultMaxConnectionsPerHost        = 240
	defaultHTTP2ReadIdleTimeoutSeconds  = 10
)

// 模型嗅探上限：state 头按模型区分，模型名只能从出站请求体的 JSON 里取，
// 因此转发前要先缓冲请求体开头这么多字节来找 "model" 字段。
const (
	DefaultModelSniffMaxBytes = 256 << 10
	MinModelSniffMaxBytes     = 1 << 10
	MaxModelSniffMaxBytes     = 1 << 20
)

// 配置体量上限，防止单条配置膨胀到影响宿主加密存储。
const (
	MaxAccounts   = 500
	MaxPoolModels = 16
	// MaxModelStateLengths 是「按模型覆盖满血长度」的条数上限。比 MaxPoolModels 宽：
	// 覆盖表也可以给 follow_observed_models 自动跟踪出来的模型预设长度。
	MaxModelStateLengths  = 32
	MaxProxyItems         = 200
	maxExtraHeaderEntries = 64
	maxHeaderNameBytes    = 200
	maxHeaderValueBytes   = 8192
	maxURLBytes           = 2048
	maxAccountNoteBytes   = 120
	maxProxyNameBytes     = 40
	maxUserAgentBytes     = 200
	// MaxModelNameBytes 同时约束配置里的 models 与从请求体嗅探到的模型名。
	MaxModelNameBytes = 120
)

// 覆盖模式与「取不到值」时的策略。on_unavailable 与 on_model_unmatched 共用同一组取值。
const (
	OverrideModeAlways      = "always"
	OverrideModeFillMissing = "fill_missing"

	OnUnavailablePassthrough = "passthrough"
	OnUnavailableFailRequest = "fail_request"

	ProxyModeHost     = "host"
	ProxyModeDisabled = "disabled"
)

// 撞票探针的推理强度。none 表示请求体里不带 reasoning 字段。
const (
	EffortNone    = "none"
	EffortMinimal = "minimal"
	EffortLow     = "low"
	EffortMedium  = "medium"
	EffortHigh    = "high"
)

// 代理条目支持的协议，与 transport 的拨号能力一一对应。
// DefaultProxyScheme 是省略协议时的补全值：手上的代理绝大多数是 socks5。
const (
	ProxySchemeHTTP    = "http"
	ProxySchemeHTTPS   = "https"
	ProxySchemeSOCKS5  = "socks5"
	ProxySchemeSOCKS5H = "socks5h"

	DefaultProxyScheme = ProxySchemeSOCKS5
)

// 票池默认值全部对齐参考实现 codex-ticket-pool（backend/app/config.py）。
//
// 满血票的长度因模型而异，实测：gpt-5.5 是 292，gpt-5.6-sol/terra/luna 与 gpt-6-astra
// 都是 312。所以 DefaultTargetStateLength 只是「没有单独配置的模型」的兜底值，
// premium 模型由 DefaultModelStateLengths 单独给 312。
const (
	DefaultGatewayBaseURL = "https://chatgpt.com/backend-api/codex"
	DefaultUserAgent      = "codex_cli_rs/0.154.0"
	// DefaultTargetStateLength 是 gpt-5.5 档的满血长度，同时作为未覆盖模型的兜底。
	DefaultTargetStateLength = 292
	// DefaultPremiumStateLength 是 5.6 / 6 系列的满血长度。
	DefaultPremiumStateLength     = 312
	DefaultPoolSize               = 5
	DefaultTicketTTLSeconds       = 2700
	DefaultTicketStaggerSeconds   = 600
	DefaultRefillThresholdSeconds = 600
	DefaultCheckIntervalSeconds   = 30
	DefaultProbeTimeoutSeconds    = 45
	DefaultProxiesPerRound        = 4
	DefaultRetryRounds            = 3

	MinCheckIntervalSeconds = 10
	MinTicketTTLSeconds     = 300
)

// 各协议在地址省略端口时补全的默认端口。
var defaultProxyPorts = map[string]string{
	ProxySchemeHTTP:    "80",
	ProxySchemeHTTPS:   "443",
	ProxySchemeSOCKS5:  "1080",
	ProxySchemeSOCKS5H: "1080",
}

// Config 是插件的完整配置。所有字段在规范化后一定有值。
type Config struct {
	RequestTimeoutSeconds        int               `json:"request_timeout_seconds"`
	ResponseHeaderTimeoutSeconds int               `json:"response_header_timeout_seconds"`
	DialTimeoutSeconds           int               `json:"dial_timeout_seconds"`
	TLSHandshakeTimeoutSeconds   int               `json:"tls_handshake_timeout_seconds"`
	IdleConnectionTimeoutSeconds int               `json:"idle_connection_timeout_seconds"`
	MaxIdleConnections           int               `json:"max_idle_connections"`
	MaxIdleConnectionsPerHost    int               `json:"max_idle_connections_per_host"`
	MaxConnectionsPerHost        int               `json:"max_connections_per_host"`
	EnableHTTP2                  bool              `json:"enable_http2"`
	HTTP2ReadIdleTimeoutSeconds  int               `json:"http2_read_idle_timeout_seconds"`
	TLSMinVersion                string            `json:"tls_min_version"`
	ProxyMode                    string            `json:"proxy_mode"`
	ExtraHeaders                 map[string]string `json:"extra_headers"`
	OverloadGuard                Guard             `json:"overload_guard"`
}

// Guard 是过载防护本身的配置。
type Guard struct {
	Enabled       bool   `json:"enabled"`
	HeaderName    string `json:"header_name"`
	OverrideMode  string `json:"override_mode"`
	OnUnavailable string `json:"on_unavailable"`
	// OnModelUnmatched 决定「有票但没有当前模型的票」（含解析不出模型名）时的行为。
	// state 头按模型铸造，跨模型复用会被上游识破，所以这里绝不会退化成"随便拿一张票"。
	OnModelUnmatched string `json:"on_model_unmatched"`
	// ModelSniffMaxBytes 是为找出 model 字段而缓冲的请求体字节数上限。
	ModelSniffMaxBytes int       `json:"model_sniff_max_bytes"`
	TicketPool         Ticket    `json:"ticket_pool"`
	ProxyPool          Proxy     `json:"proxy_pool"`
	Accounts           []Account `json:"accounts"`
}

// Ticket 是插件自建票池的参数：撞票探针怎么打、票怎么判定、池怎么补。
//
// 全部字段的语义与默认值来自参考实现 codex-ticket-pool，见各常量注释。
type Ticket struct {
	// Models 是每个账号默认要维护票池的模型。
	Models []string `json:"models"`
	// FollowObservedModels 为真时，过路请求里出现的新模型也会自动获得一个票池。
	// 这是「配置里没写全模型」时唯一的兜底：否则那个模型永远取不到票。
	FollowObservedModels bool `json:"follow_observed_models"`
	// PoolSize 是每个（账号 × 模型）池维持的满血票张数。
	PoolSize int `json:"pool_size"`
	// TargetStateLength 是 ModelStateLengths 里没有单列的模型的满血票长度标准；
	// 0 表示不按长度判定（任何带产出的票都入池）。
	TargetStateLength int `json:"target_state_length"`
	// ModelStateLengths 按模型覆盖满血票长度。满血长度因模型而异（gpt-5.5 是 292，
	// premium 是 312），单一数字必然对其中一类是错的：口径取小了 premium 池只能收到
	// 低一档的票，取大了 5.5 池一张也收不到。值为 0 表示该模型不按长度判定。
	ModelStateLengths map[string]int `json:"model_state_lengths"`
	// TicketTTLSeconds 是票入池后的有效期。上游不告诉我们真实有效期，这是经验值。
	TicketTTLSeconds int `json:"ticket_ttl_seconds"`
	// TicketStaggerSeconds 让同批入池的票逐张提前过期，避免整池同时到期。
	TicketStaggerSeconds int `json:"ticket_stagger_seconds"`
	// RefillThresholdSeconds 同时是「池未满时的补池冷却」与「池已满时的续期阈值」。
	RefillThresholdSeconds int `json:"refill_threshold_seconds"`
	// CheckIntervalSeconds 是上一轮补池结束到下一轮开始的间隔。
	CheckIntervalSeconds int `json:"check_interval_seconds"`
	ProbeTimeoutSeconds  int `json:"probe_timeout_seconds"`
	// ProbeEffort 是探针请求体里的 reasoning.effort，none 表示不带该字段。
	ProbeEffort string `json:"probe_effort"`
	// ProxiesPerRound 是一轮撞票最多并用几个代理。
	ProxiesPerRound int `json:"proxies_per_round"`
	// RetryRounds 是一轮没撞到满血票时，换未用过的代理再试几轮。
	RetryRounds int `json:"retry_rounds"`
	// IncludeDirect 决定每轮是否也用直连（不经代理）打一发。
	IncludeDirect bool `json:"include_direct"`
	// GatewayBaseURL 是撞票用的网关地址。过路请求里嗅探到的地址优先于它。
	GatewayBaseURL string `json:"gateway_base_url"`
	// UserAgent 是探针的 UA 兜底值；过路请求带了 UA 时优先沿用真实客户端的。
	UserAgent string `json:"user_agent"`
}

// Proxy 是代理库的配置：撞票探针可以用的出口，由管理员直接填写。
//
// 这里不做连通性测试也不自动剔除——填了就用。某条代理是死是活，看它在撞票里的
// 实际成败（见 proxypool.Status），而不是插件替上游先探一遍。
type Proxy struct {
	Enabled bool        `json:"enabled"`
	Proxies []ProxyItem `json:"proxies"`
}

// ProxyItem 是一条代理。
//
// URL 形如 socks5://1.2.3.4:1080 或 http://user:pass@10.0.0.9:8080：
// 协议支持 http/https/socks5/socks5h，省略协议按 socks5 处理，省略端口按协议补默认端口。
type ProxyItem struct {
	// Name 只用于配置页和状态看板的展示，可以留空。
	Name    string `json:"name"`
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
}

// Account 是账号级过载防护开关。
//
// 这里不再有任何 Provider 字段：票由插件自己撞，账号的 OAuth 凭据从过路请求里取。
type Account struct {
	AccountID int64  `json:"account_id"`
	Enabled   bool   `json:"enabled"`
	Note      string `json:"note"`
}

// Default 返回全默认配置：过载防护开启但还没有账号被接管，因此不会注入任何头，
// 也不会有任何撞票流量。
func Default() Config {
	return Config{
		RequestTimeoutSeconds:        0,
		ResponseHeaderTimeoutSeconds: defaultResponseHeaderTimeoutSeconds,
		DialTimeoutSeconds:           defaultDialTimeoutSeconds,
		TLSHandshakeTimeoutSeconds:   defaultTLSHandshakeTimeoutSeconds,
		IdleConnectionTimeoutSeconds: defaultIdleConnectionTimeoutSeconds,
		MaxIdleConnections:           defaultMaxIdleConnections,
		MaxIdleConnectionsPerHost:    defaultMaxIdleConnectionsPerHost,
		MaxConnectionsPerHost:        defaultMaxConnectionsPerHost,
		EnableHTTP2:                  true,
		HTTP2ReadIdleTimeoutSeconds:  defaultHTTP2ReadIdleTimeoutSeconds,
		TLSMinVersion:                "1.2",
		ProxyMode:                    ProxyModeHost,
		ExtraHeaders:                 map[string]string{},
		OverloadGuard: Guard{
			Enabled:            true,
			HeaderName:         DefaultHeaderName,
			OverrideMode:       OverrideModeAlways,
			OnUnavailable:      OnUnavailablePassthrough,
			OnModelUnmatched:   OnUnavailablePassthrough,
			ModelSniffMaxBytes: DefaultModelSniffMaxBytes,
			TicketPool:         DefaultTicketPool(),
			ProxyPool:          DefaultProxyPool(),
			Accounts:           []Account{},
		},
	}
}

// DefaultTicketPool 返回票池默认参数。
func DefaultTicketPool() Ticket {
	return Ticket{
		Models:                 DefaultPoolModels(),
		FollowObservedModels:   true,
		PoolSize:               DefaultPoolSize,
		TargetStateLength:      DefaultTargetStateLength,
		ModelStateLengths:      DefaultModelStateLengths(),
		TicketTTLSeconds:       DefaultTicketTTLSeconds,
		TicketStaggerSeconds:   DefaultTicketStaggerSeconds,
		RefillThresholdSeconds: DefaultRefillThresholdSeconds,
		CheckIntervalSeconds:   DefaultCheckIntervalSeconds,
		ProbeTimeoutSeconds:    DefaultProbeTimeoutSeconds,
		ProbeEffort:            EffortLow,
		ProxiesPerRound:        DefaultProxiesPerRound,
		RetryRounds:            DefaultRetryRounds,
		IncludeDirect:          true,
		GatewayBaseURL:         DefaultGatewayBaseURL,
		UserAgent:              DefaultUserAgent,
	}
}

// DefaultPoolModels 返回默认要维护票池的模型，与 codex-ticket-pool 的 POOL_MODELS 一致。
func DefaultPoolModels() []string {
	return []string{"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-6-astra"}
}

// DefaultModelStateLengths 返回默认的按模型满血长度覆盖表。
//
// 默认池里那几个模型都是 5.6/6 系列，满血长度实测都是 312，和全局兜底的 292 不同；
// 不预置这张表的话，它们的池只能收到 292 的票——那是低一档的票，注进去等于把降智钉死。
func DefaultModelStateLengths() map[string]int {
	out := make(map[string]int, len(DefaultPoolModels()))
	for _, model := range DefaultPoolModels() {
		out[model] = DefaultPremiumStateLength
	}
	return out
}

// TargetLengthFor 返回某个模型的满血票长度：覆盖表里有就用它，否则用全局兜底值。
func (t Ticket) TargetLengthFor(model string) int {
	if length, ok := t.ModelStateLengths[model]; ok {
		return length
	}
	return t.TargetStateLength
}

// DefaultProxyPool 返回代理库默认参数：开着，但一条代理都没有。
//
// 代理是管理员自己的资源，插件不预置任何地址；列表为空时撞票只走直连。
func DefaultProxyPool() Proxy {
	return Proxy{
		Enabled: true,
		Proxies: []ProxyItem{},
	}
}

// Parse 严格解析配置 JSON：拒绝未知字段、拒绝多个 JSON 值，缺省字段补默认值，
// 校验失败返回可直接展示给管理员的错误。
func Parse(raw []byte) (Config, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return Default(), nil
	}
	var wire wireConfig
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Config{}, fmt.Errorf("解析配置失败: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("配置只能包含一个 JSON 对象")
	}
	config := wire.merge(Default())
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Marshal 输出规范化配置字节，供 ValidateConfig 返回给宿主保存。
func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Clone 深拷贝配置，避免运行中的实例与新配置共享 map/slice。
func (c Config) Clone() Config {
	out := c
	out.ExtraHeaders = cloneHeaders(c.ExtraHeaders)
	out.OverloadGuard.TicketPool.Models = append([]string(nil), c.OverloadGuard.TicketPool.Models...)
	out.OverloadGuard.TicketPool.ModelStateLengths = cloneModelLengths(c.OverloadGuard.TicketPool.ModelStateLengths)
	out.OverloadGuard.ProxyPool.Proxies = append([]ProxyItem(nil), c.OverloadGuard.ProxyPool.Proxies...)
	out.OverloadGuard.Accounts = append([]Account(nil), c.OverloadGuard.Accounts...)
	return out
}

// SniffLimit 返回为解析模型名而缓冲的请求体字节上限，缺省时回落到默认值。
func (g Guard) SniffLimit() int {
	if g.ModelSniffMaxBytes < MinModelSniffMaxBytes || g.ModelSniffMaxBytes > MaxModelSniffMaxBytes {
		return DefaultModelSniffMaxBytes
	}
	return g.ModelSniffMaxBytes
}

// EnabledAccounts 返回所有实际生效的账号条目，按账号 ID 升序，供票池建表。
func (c Config) EnabledAccounts() []Account {
	if !c.OverloadGuard.Enabled {
		return nil
	}
	out := make([]Account, 0, len(c.OverloadGuard.Accounts))
	for _, account := range c.OverloadGuard.Accounts {
		if !account.Enabled || account.AccountID <= 0 {
			continue
		}
		out = append(out, account)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccountID < out[j].AccountID })
	return out
}

// AccountEnabled 判断某个账号是否被过载防护接管。
func (c Config) AccountEnabled(accountID int64) bool {
	if !c.OverloadGuard.Enabled {
		return false
	}
	for _, account := range c.OverloadGuard.Accounts {
		if account.AccountID == accountID {
			return account.Enabled
		}
	}
	return false
}

// ActiveProxies 返回启用的代理条目。代理池总开关关掉时一条都不返回。
func (p Proxy) ActiveProxies() []ProxyItem {
	if !p.Enabled {
		return nil
	}
	out := make([]ProxyItem, 0, len(p.Proxies))
	for _, item := range p.Proxies {
		if item.Enabled {
			out = append(out, item)
		}
	}
	return out
}

// ProxyScheme 返回代理条目的协议（已规范化为小写），解析不出来时返回空串。
func (p ProxyItem) ProxyScheme() string {
	scheme, _, found := strings.Cut(p.URL, "://")
	if !found {
		return ""
	}
	return strings.ToLower(scheme)
}

func cloneHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for name, value := range headers {
		out[name] = value
	}
	return out
}

func cloneModelLengths(lengths map[string]int) map[string]int {
	out := make(map[string]int, len(lengths))
	for model, length := range lengths {
		out[model] = length
	}
	return out
}
