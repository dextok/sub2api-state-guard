package pluginconfig

import "encoding/json"

// 线格式全部使用指针字段：区分「没写这个键」与「显式写了零值」。
// 未出现的键保留 Default() 的值，出现的键（哪怕是 0/false/""）一律覆盖，
// 再由 normalize/validate 决定该值是否可接受。
type wireConfig struct {
	RequestTimeoutSeconds        *int               `json:"request_timeout_seconds"`
	ResponseHeaderTimeoutSeconds *int               `json:"response_header_timeout_seconds"`
	DialTimeoutSeconds           *int               `json:"dial_timeout_seconds"`
	TLSHandshakeTimeoutSeconds   *int               `json:"tls_handshake_timeout_seconds"`
	IdleConnectionTimeoutSeconds *int               `json:"idle_connection_timeout_seconds"`
	MaxIdleConnections           *int               `json:"max_idle_connections"`
	MaxIdleConnectionsPerHost    *int               `json:"max_idle_connections_per_host"`
	MaxConnectionsPerHost        *int               `json:"max_connections_per_host"`
	EnableHTTP2                  *bool              `json:"enable_http2"`
	HTTP2ReadIdleTimeoutSeconds  *int               `json:"http2_read_idle_timeout_seconds"`
	TLSMinVersion                *string            `json:"tls_min_version"`
	ProxyMode                    *string            `json:"proxy_mode"`
	ExtraHeaders                 *map[string]string `json:"extra_headers"`
	OverloadGuard                *wireGuard         `json:"overload_guard"`
}

type wireGuard struct {
	Enabled            *bool          `json:"enabled"`
	HeaderName         *string        `json:"header_name"`
	OverrideMode       *string        `json:"override_mode"`
	OnUnavailable      *string        `json:"on_unavailable"`
	OnModelUnmatched   *string        `json:"on_model_unmatched"`
	ModelSniffMaxBytes *int           `json:"model_sniff_max_bytes"`
	TicketPool         *wireTicket    `json:"ticket_pool"`
	ProxyPool          *wireProxy     `json:"proxy_pool"`
	Accounts           *[]wireAccount `json:"accounts"`

	// 已移除的字段：只保留解析入口，值一律忽略。
	// 旧版本存下来的配置里还有这些键，而解析是 DisallowUnknownFields 的——
	// 若直接删掉，升级后第一次 ApplyConfig 就会因"未知字段"整体失败。
	// 用 RawMessage 兜底是因为旧结构本身也可能变过，这里不关心它长什么样。
	DefaultProvider *json.RawMessage `json:"default_provider"`
	ModelAliases    *json.RawMessage `json:"model_aliases"`
}

type wireTicket struct {
	Models                 *[]string       `json:"models"`
	FollowObservedModels   *bool           `json:"follow_observed_models"`
	PoolSize               *int            `json:"pool_size"`
	TargetStateLength      *int            `json:"target_state_length"`
	ModelStateLengths      *map[string]int `json:"model_state_lengths"`
	TicketTTLSeconds       *int            `json:"ticket_ttl_seconds"`
	TicketStaggerSeconds   *int            `json:"ticket_stagger_seconds"`
	RefillThresholdSeconds *int            `json:"refill_threshold_seconds"`
	CheckIntervalSeconds   *int            `json:"check_interval_seconds"`
	ProbeTimeoutSeconds    *int            `json:"probe_timeout_seconds"`
	ProbeEffort            *string         `json:"probe_effort"`
	ProxiesPerRound        *int            `json:"proxies_per_round"`
	RetryRounds            *int            `json:"retry_rounds"`
	IncludeDirect          *bool           `json:"include_direct"`
	GatewayBaseURL         *string         `json:"gateway_base_url"`
	UserAgent              *string         `json:"user_agent"`
}

type wireProxy struct {
	Enabled *bool            `json:"enabled"`
	Proxies *[]wireProxyItem `json:"proxies"`

	// 代理池早期是「按接口拉取 socks5 列表」，这些键已经移除；接受并忽略，
	// 否则装着旧配置的实例一升级就会因为未知字段直接解析失败。
	Sources                *json.RawMessage `json:"sources"`
	RefreshIntervalSeconds *json.RawMessage `json:"refresh_interval_seconds"`
	TestURL                *json.RawMessage `json:"test_url"`
	TestTimeoutSeconds     *json.RawMessage `json:"test_timeout_seconds"`
	FetchTimeoutSeconds    *json.RawMessage `json:"fetch_timeout_seconds"`
	TTLSeconds             *json.RawMessage `json:"ttl_seconds"`
	MaxUseFails            *json.RawMessage `json:"max_use_fails"`
	MaxConsecutiveFails    *json.RawMessage `json:"max_consecutive_fails"`
	MaxSize                *json.RawMessage `json:"max_size"`
}

type wireProxyItem struct {
	Name    *string `json:"name"`
	URL     *string `json:"url"`
	Enabled *bool   `json:"enabled"`

	// 同上：旧的代理源条目字段，接受并忽略。
	Count *json.RawMessage `json:"count"`
	Mode  *json.RawMessage `json:"mode"`
}

type wireAccount struct {
	AccountID *int64  `json:"account_id"`
	Enabled   *bool   `json:"enabled"`
	Note      *string `json:"note"`

	// 已移除的字段，同上：接受并忽略。
	ProviderAccountID *json.RawMessage `json:"provider_account_id"`
	Provider          *json.RawMessage `json:"provider"`
}

func (w wireConfig) merge(base Config) Config {
	out := base
	mergeInt(&out.RequestTimeoutSeconds, w.RequestTimeoutSeconds)
	mergeInt(&out.ResponseHeaderTimeoutSeconds, w.ResponseHeaderTimeoutSeconds)
	mergeInt(&out.DialTimeoutSeconds, w.DialTimeoutSeconds)
	mergeInt(&out.TLSHandshakeTimeoutSeconds, w.TLSHandshakeTimeoutSeconds)
	mergeInt(&out.IdleConnectionTimeoutSeconds, w.IdleConnectionTimeoutSeconds)
	mergeInt(&out.MaxIdleConnections, w.MaxIdleConnections)
	mergeInt(&out.MaxIdleConnectionsPerHost, w.MaxIdleConnectionsPerHost)
	mergeInt(&out.MaxConnectionsPerHost, w.MaxConnectionsPerHost)
	mergeBool(&out.EnableHTTP2, w.EnableHTTP2)
	mergeInt(&out.HTTP2ReadIdleTimeoutSeconds, w.HTTP2ReadIdleTimeoutSeconds)
	mergeString(&out.TLSMinVersion, w.TLSMinVersion)
	mergeString(&out.ProxyMode, w.ProxyMode)
	mergeHeaders(&out.ExtraHeaders, w.ExtraHeaders)
	if w.OverloadGuard != nil {
		out.OverloadGuard = w.OverloadGuard.merge(out.OverloadGuard)
	}
	return out
}

func (w wireGuard) merge(base Guard) Guard {
	out := base
	mergeBool(&out.Enabled, w.Enabled)
	mergeString(&out.HeaderName, w.HeaderName)
	mergeString(&out.OverrideMode, w.OverrideMode)
	mergeString(&out.OnUnavailable, w.OnUnavailable)
	mergeString(&out.OnModelUnmatched, w.OnModelUnmatched)
	mergeInt(&out.ModelSniffMaxBytes, w.ModelSniffMaxBytes)
	if w.TicketPool != nil {
		out.TicketPool = w.TicketPool.merge(out.TicketPool)
	}
	if w.ProxyPool != nil {
		out.ProxyPool = w.ProxyPool.merge(out.ProxyPool)
	}
	if w.Accounts != nil {
		accounts := make([]Account, 0, len(*w.Accounts))
		for _, item := range *w.Accounts {
			accounts = append(accounts, item.merge())
		}
		out.Accounts = accounts
	}
	return out
}

func (w wireTicket) merge(base Ticket) Ticket {
	out := base
	if w.Models != nil {
		out.Models = append([]string(nil), (*w.Models)...)
	}
	mergeBool(&out.FollowObservedModels, w.FollowObservedModels)
	mergeInt(&out.PoolSize, w.PoolSize)
	mergeInt(&out.TargetStateLength, w.TargetStateLength)
	// 显式给了这个键就整表替换（包括给成 {} 表示"全都跟随全局值"）；没给才保留默认表。
	if w.ModelStateLengths != nil {
		out.ModelStateLengths = cloneModelLengths(*w.ModelStateLengths)
	}
	mergeInt(&out.TicketTTLSeconds, w.TicketTTLSeconds)
	mergeInt(&out.TicketStaggerSeconds, w.TicketStaggerSeconds)
	mergeInt(&out.RefillThresholdSeconds, w.RefillThresholdSeconds)
	mergeInt(&out.CheckIntervalSeconds, w.CheckIntervalSeconds)
	mergeInt(&out.ProbeTimeoutSeconds, w.ProbeTimeoutSeconds)
	mergeString(&out.ProbeEffort, w.ProbeEffort)
	mergeInt(&out.ProxiesPerRound, w.ProxiesPerRound)
	mergeInt(&out.RetryRounds, w.RetryRounds)
	mergeBool(&out.IncludeDirect, w.IncludeDirect)
	mergeString(&out.GatewayBaseURL, w.GatewayBaseURL)
	mergeString(&out.UserAgent, w.UserAgent)
	return out
}

func (w wireProxy) merge(base Proxy) Proxy {
	out := base
	mergeBool(&out.Enabled, w.Enabled)
	if w.Proxies != nil {
		// 列表整体替换而不是逐项合并：管理员删掉一行就该真的没了。
		// 但单行内部仍然继承默认值，少写 enabled 不至于被当成停用。
		items := make([]ProxyItem, 0, len(*w.Proxies))
		for _, item := range *w.Proxies {
			items = append(items, item.merge())
		}
		out.Proxies = items
	}
	return out
}

func (w wireProxyItem) merge() ProxyItem {
	out := ProxyItem{Enabled: true}
	mergeString(&out.Name, w.Name)
	mergeString(&out.URL, w.URL)
	mergeBool(&out.Enabled, w.Enabled)
	return out
}

func (w wireAccount) merge() Account {
	out := Account{Enabled: true}
	mergeInt64(&out.AccountID, w.AccountID)
	mergeBool(&out.Enabled, w.Enabled)
	mergeString(&out.Note, w.Note)
	return out
}

func mergeInt(dst *int, src *int) {
	if src != nil {
		*dst = *src
	}
}

func mergeInt64(dst *int64, src *int64) {
	if src != nil {
		*dst = *src
	}
}

func mergeBool(dst *bool, src *bool) {
	if src != nil {
		*dst = *src
	}
}

func mergeString(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}

func mergeHeaders(dst *map[string]string, src *map[string]string) {
	if src == nil {
		return
	}
	out := make(map[string]string, len(*src))
	for name, value := range *src {
		out[name] = value
	}
	*dst = out
}
