package pluginconfig

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// headerScope 决定某个头名称受哪一套规则约束：注入到 OpenAI 上游的头必须避开
// 宿主的受保护名单，而 header_name 本身是本插件的目的，需要单独放行。
type headerScope int

const (
	scopeUpstream headerScope = iota
	scopeGuardHeader
)

// headerOverrideBlockedNames 镜像宿主账号级 header override 的黑名单
// （frontend/src/components/account/credentialsBuilder.ts），避免插件成为绕过
// 宿主保护的后门。header_name 允许 x-codex-turn-state——那正是本插件的目的。
var headerOverrideBlockedNames = map[string]struct{}{
	"host": {}, "content-length": {}, "content-type": {}, "transfer-encoding": {},
	"connection": {}, "keep-alive": {}, "proxy-authenticate": {}, "proxy-authorization": {},
	"proxy-connection": {}, "te": {}, "trailer": {}, "upgrade": {},
	"authorization": {}, "x-api-key": {}, "x-goog-api-key": {}, "cookie": {},
	"accept-encoding": {}, "sec-websocket-key": {}, "sec-websocket-version": {},
	"sec-websocket-extensions": {}, "sec-websocket-protocol": {}, "sec-websocket-accept": {},
	"session_id": {}, "conversation_id": {},
	"x-codex-turn-state": {}, "x-codex-turn-metadata": {}, "chatgpt-account-id": {},
	"x-claude-code-session-id": {}, "x-client-request-id": {}, "x-grok-conv-id": {},
}

// normalize 统一大小写与空白，让相同语义的配置有唯一表示。
func (c *Config) normalize() {
	c.TLSMinVersion = strings.TrimSpace(c.TLSMinVersion)
	c.ProxyMode = strings.ToLower(strings.TrimSpace(c.ProxyMode))
	c.ExtraHeaders = normalizeHeaders(c.ExtraHeaders)

	guard := &c.OverloadGuard
	guard.HeaderName = strings.TrimSpace(guard.HeaderName)
	if guard.HeaderName == "" {
		guard.HeaderName = DefaultHeaderName
	}
	// 三个策略字段留空一律取默认值，与其它字段的「空即默认」口径一致。
	guard.OverrideMode = strings.ToLower(strings.TrimSpace(guard.OverrideMode))
	if guard.OverrideMode == "" {
		guard.OverrideMode = OverrideModeAlways
	}
	guard.OnUnavailable = strings.ToLower(strings.TrimSpace(guard.OnUnavailable))
	if guard.OnUnavailable == "" {
		guard.OnUnavailable = OnUnavailablePassthrough
	}
	guard.OnModelUnmatched = strings.ToLower(strings.TrimSpace(guard.OnModelUnmatched))
	if guard.OnModelUnmatched == "" {
		guard.OnModelUnmatched = OnUnavailablePassthrough
	}
	if guard.ModelSniffMaxBytes == 0 {
		guard.ModelSniffMaxBytes = DefaultModelSniffMaxBytes
	}
	guard.TicketPool.normalize()
	guard.ProxyPool.normalize()
	if guard.Accounts == nil {
		guard.Accounts = []Account{}
	}
	for index := range guard.Accounts {
		guard.Accounts[index].Note = strings.TrimSpace(guard.Accounts[index].Note)
	}
}

func (t *Ticket) normalize() {
	// 模型名去空、去重、保序：管理员从界面粘进来的列表经常带空行和重复项。
	seen := make(map[string]struct{}, len(t.Models))
	models := make([]string, 0, len(t.Models))
	for _, model := range t.Models {
		trimmed := strings.TrimSpace(model)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		models = append(models, trimmed)
	}
	t.Models = models

	t.ProbeEffort = strings.ToLower(strings.TrimSpace(t.ProbeEffort))
	if t.ProbeEffort == "" {
		t.ProbeEffort = EffortLow
	}
	t.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(t.GatewayBaseURL), "/")
	if t.GatewayBaseURL == "" {
		t.GatewayBaseURL = DefaultGatewayBaseURL
	}
	t.UserAgent = strings.TrimSpace(t.UserAgent)
	if t.UserAgent == "" {
		t.UserAgent = DefaultUserAgent
	}
}

// normalize 规范化代理条目。必须幂等：ValidateConfig 会把规范化结果回存给宿主，
// 而 TestConfig 又靠「两次 Marshal 是否一致」判断配置有没有生效。
func (p *Proxy) normalize() {
	if p.Proxies == nil {
		p.Proxies = []ProxyItem{}
	}
	for index := range p.Proxies {
		item := &p.Proxies[index]
		item.Name = strings.TrimSpace(item.Name)
		item.URL = normalizeProxyURL(item.URL)
	}
}

// normalizeProxyURL 把管理员填的地址补成完整的代理 URL：
// 省略协议按 socks5（手上的代理绝大多数是它），省略端口按协议补默认端口。
//
// 认不出来的输入原样返回，交给 validate 报一个能看懂的错。
func normalizeProxyURL(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") {
		value = DefaultProxyScheme + "://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return value
	}
	scheme := strings.ToLower(parsed.Scheme)
	parsed.Scheme = scheme
	if parsed.Host != "" && parsed.Port() == "" {
		if port, ok := defaultProxyPorts[scheme]; ok {
			parsed.Host = net.JoinHostPort(strings.Trim(parsed.Host, "[]"), port)
		}
	}
	// 代理地址只有 scheme://user:pass@host:port 有意义，路径/查询/锚点一律丢掉。
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func normalizeHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for name, value := range headers {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		out[trimmed] = value
	}
	return out
}

func (c *Config) validate() error {
	c.normalize()

	if c.RequestTimeoutSeconds != 0 && (c.RequestTimeoutSeconds < 60 || c.RequestTimeoutSeconds > 86400) {
		return fmt.Errorf("request_timeout_seconds 必须为 0（不限制，流式响应推荐）或 60-86400，当前 %d", c.RequestTimeoutSeconds)
	}
	if err := checkRange("response_header_timeout_seconds", c.ResponseHeaderTimeoutSeconds, 1, 3600); err != nil {
		return err
	}
	if err := checkRange("dial_timeout_seconds", c.DialTimeoutSeconds, 1, 120); err != nil {
		return err
	}
	if err := checkRange("tls_handshake_timeout_seconds", c.TLSHandshakeTimeoutSeconds, 1, 120); err != nil {
		return err
	}
	if err := checkRange("idle_connection_timeout_seconds", c.IdleConnectionTimeoutSeconds, 1, 3600); err != nil {
		return err
	}
	if err := checkRange("max_idle_connections", c.MaxIdleConnections, 1, 10000); err != nil {
		return err
	}
	if err := checkRange("max_idle_connections_per_host", c.MaxIdleConnectionsPerHost, 1, c.MaxIdleConnections); err != nil {
		return err
	}
	if err := checkRange("max_connections_per_host", c.MaxConnectionsPerHost, 0, 10000); err != nil {
		return err
	}
	if c.MaxConnectionsPerHost != 0 && c.MaxConnectionsPerHost < c.MaxIdleConnectionsPerHost {
		return fmt.Errorf("max_connections_per_host(%d) 不能小于 max_idle_connections_per_host(%d)",
			c.MaxConnectionsPerHost, c.MaxIdleConnectionsPerHost)
	}
	if err := checkRange("http2_read_idle_timeout_seconds", c.HTTP2ReadIdleTimeoutSeconds, 0, 600); err != nil {
		return err
	}
	if c.TLSMinVersion != "1.2" && c.TLSMinVersion != "1.3" {
		return fmt.Errorf(`tls_min_version 只支持 "1.2" 或 "1.3"，当前 %q`, c.TLSMinVersion)
	}
	if c.ProxyMode != ProxyModeHost && c.ProxyMode != ProxyModeDisabled {
		return fmt.Errorf(`proxy_mode 只支持 %q 或 %q，当前 %q`, ProxyModeHost, ProxyModeDisabled, c.ProxyMode)
	}
	if err := validateHeaderMap("extra_headers", c.ExtraHeaders, scopeUpstream); err != nil {
		return err
	}

	guard := c.OverloadGuard
	if err := validateHeaderName("overload_guard.header_name", guard.HeaderName, scopeGuardHeader); err != nil {
		return err
	}
	if guard.OverrideMode != OverrideModeAlways && guard.OverrideMode != OverrideModeFillMissing {
		return fmt.Errorf(`overload_guard.override_mode 只支持 %q 或 %q，当前 %q`,
			OverrideModeAlways, OverrideModeFillMissing, guard.OverrideMode)
	}
	if guard.OnUnavailable != OnUnavailablePassthrough && guard.OnUnavailable != OnUnavailableFailRequest {
		return fmt.Errorf(`overload_guard.on_unavailable 只支持 %q 或 %q，当前 %q`,
			OnUnavailablePassthrough, OnUnavailableFailRequest, guard.OnUnavailable)
	}
	if guard.OnModelUnmatched != OnUnavailablePassthrough && guard.OnModelUnmatched != OnUnavailableFailRequest {
		return fmt.Errorf(`overload_guard.on_model_unmatched 只支持 %q 或 %q，当前 %q`,
			OnUnavailablePassthrough, OnUnavailableFailRequest, guard.OnModelUnmatched)
	}
	if err := checkRange("overload_guard.model_sniff_max_bytes", guard.ModelSniffMaxBytes,
		MinModelSniffMaxBytes, MaxModelSniffMaxBytes); err != nil {
		return err
	}
	if err := guard.TicketPool.validate("overload_guard.ticket_pool"); err != nil {
		return err
	}
	if err := guard.ProxyPool.validate("overload_guard.proxy_pool"); err != nil {
		return err
	}
	if err := validateAccounts(guard.Accounts); err != nil {
		return err
	}

	// 既不允许直连、又没有任何可用代理（或每轮一条代理都不用），撞票永远发不出去——
	// 这是配置错误而不是运行时故障，在保存时就拦下来，比让管理员盯着一个永远空着的票池排查要快。
	if guard.Enabled && hasEnabledAccount(guard.Accounts) && !guard.TicketPool.IncludeDirect {
		if len(guard.ProxyPool.ActiveProxies()) == 0 {
			return fmt.Errorf("已开启过载防护的账号无法撞票：overload_guard.ticket_pool.include_direct 为 false，" +
				"且 overload_guard.proxy_pool 没有启用任何代理")
		}
		if guard.TicketPool.ProxiesPerRound == 0 {
			return fmt.Errorf("已开启过载防护的账号无法撞票：overload_guard.ticket_pool.include_direct 为 false，" +
				"且 overload_guard.ticket_pool.proxies_per_round 为 0")
		}
	}
	return nil
}

func validateAccounts(accounts []Account) error {
	if len(accounts) > MaxAccounts {
		return fmt.Errorf("overload_guard.accounts 最多 %d 条，当前 %d 条", MaxAccounts, len(accounts))
	}
	seen := make(map[int64]struct{}, len(accounts))
	for index, account := range accounts {
		field := fmt.Sprintf("overload_guard.accounts[%d]", index)
		if account.AccountID <= 0 {
			return fmt.Errorf("%s.account_id 必须是正整数", field)
		}
		if _, exists := seen[account.AccountID]; exists {
			return fmt.Errorf("账号 %d 在 overload_guard.accounts 中重复", account.AccountID)
		}
		seen[account.AccountID] = struct{}{}
		if len(account.Note) > maxAccountNoteBytes {
			return fmt.Errorf("%s.note 不能超过 %d 字节", field, maxAccountNoteBytes)
		}
		if containsControlCharacter(account.Note) {
			return fmt.Errorf("%s.note 不能包含控制字符", field)
		}
	}
	return nil
}

func hasEnabledAccount(accounts []Account) bool {
	for _, account := range accounts {
		if account.Enabled && account.AccountID > 0 {
			return true
		}
	}
	return false
}

func (t Ticket) validate(field string) error {
	if len(t.Models) > MaxPoolModels {
		return fmt.Errorf("%s.models 最多 %d 个，当前 %d 个", field, MaxPoolModels, len(t.Models))
	}
	for index, model := range t.Models {
		if err := validateModelName(fmt.Sprintf("%s.models[%d]", field, index), model); err != nil {
			return err
		}
	}
	if len(t.Models) == 0 && !t.FollowObservedModels {
		return fmt.Errorf("%s.models 为空时必须开启 follow_observed_models，否则不会为任何模型维护票池", field)
	}
	if err := checkRange(field+".pool_size", t.PoolSize, 1, 20); err != nil {
		return err
	}
	if err := checkRange(field+".ticket_ttl_seconds", t.TicketTTLSeconds, MinTicketTTLSeconds, 86400); err != nil {
		return err
	}
	if err := checkRange(field+".ticket_stagger_seconds", t.TicketStaggerSeconds, 0, 3600); err != nil {
		return err
	}
	if err := checkRange(field+".refill_threshold_seconds", t.RefillThresholdSeconds, 30, t.TicketTTLSeconds); err != nil {
		return err
	}
	if err := checkRange(field+".check_interval_seconds", t.CheckIntervalSeconds, MinCheckIntervalSeconds, 3600); err != nil {
		return err
	}
	if err := checkRange(field+".probe_timeout_seconds", t.ProbeTimeoutSeconds, 5, 300); err != nil {
		return err
	}
	switch t.ProbeEffort {
	case EffortNone, EffortMinimal, EffortLow, EffortMedium, EffortHigh:
	default:
		return fmt.Errorf("%s.probe_effort 只支持 %s/%s/%s/%s/%s，当前 %q",
			field, EffortNone, EffortMinimal, EffortLow, EffortMedium, EffortHigh, t.ProbeEffort)
	}
	if err := checkRange(field+".proxies_per_round", t.ProxiesPerRound, 0, 32); err != nil {
		return err
	}
	if err := checkRange(field+".retry_rounds", t.RetryRounds, 0, 10); err != nil {
		return err
	}
	if err := validatePlainURL(field+".gateway_base_url", t.GatewayBaseURL); err != nil {
		return err
	}
	if len(t.UserAgent) > maxUserAgentBytes {
		return fmt.Errorf("%s.user_agent 不能超过 %d 字节", field, maxUserAgentBytes)
	}
	if containsControlCharacter(t.UserAgent) {
		return fmt.Errorf("%s.user_agent 不能包含控制字符", field)
	}
	return nil
}

func (p Proxy) validate(field string) error {
	if len(p.Proxies) > MaxProxyItems {
		return fmt.Errorf("%s.proxies 最多 %d 条，当前 %d 条", field, MaxProxyItems, len(p.Proxies))
	}
	seen := make(map[string]struct{}, len(p.Proxies))
	for index, item := range p.Proxies {
		itemField := fmt.Sprintf("%s.proxies[%d]", field, index)
		if len(item.Name) > maxProxyNameBytes {
			return fmt.Errorf("%s.name 不能超过 %d 字节", itemField, maxProxyNameBytes)
		}
		if containsControlCharacter(item.Name) {
			return fmt.Errorf("%s.name 不能包含控制字符", itemField)
		}
		if err := validateProxyURL(itemField+".url", item.URL); err != nil {
			return err
		}
		if _, exists := seen[item.URL]; exists {
			return fmt.Errorf("%s.url 与前面的代理重复", itemField)
		}
		seen[item.URL] = struct{}{}
	}
	return nil
}

// validateProxyURL 校验一条代理地址。传入的地址已经过 normalizeProxyURL 补全。
//
// 出错信息里一律不回显原地址：代理 URL 可能带用户名密码，而这些错误会进日志与配置页。
func validateProxyURL(field, rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("%s 不能为空", field)
	}
	if len(rawURL) > maxURLBytes {
		return fmt.Errorf("%s 不能超过 %d 字节", field, maxURLBytes)
	}
	if containsControlCharacter(rawURL) {
		return fmt.Errorf("%s 不能包含控制字符", field)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s 不是合法的代理地址", field)
	}
	switch parsed.Scheme {
	case ProxySchemeHTTP, ProxySchemeHTTPS, ProxySchemeSOCKS5, ProxySchemeSOCKS5H:
	default:
		return fmt.Errorf("%s 的协议只支持 %s/%s/%s/%s", field,
			ProxySchemeHTTP, ProxySchemeHTTPS, ProxySchemeSOCKS5, ProxySchemeSOCKS5H)
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("%s 缺少主机名", field)
	}
	// url.Parse 对主机部分很宽松：`2001:db8::1`（没加方括号的 IPv6）会被拆成主机
	// `2001:db8:` + 端口 `1`，`socks5:1.2.3.4:1080` 补上协议后主机会变成 `socks5:1.2.3.4`。
	// 这些都要在保存时拦下，而不是等到撞票时报一个看不懂的拨号错误。
	if !validProxyHost(host) {
		return fmt.Errorf("%s 的主机必须是 IP（IPv6 需用方括号括起）或域名", field)
	}
	port := parsed.Port()
	if port == "" {
		return fmt.Errorf("%s 缺少端口", field)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("%s 的端口必须是 1-65535", field)
	}
	return nil
}

// validProxyHost 判断主机是合法 IP 或合法域名（RFC 1123 标签：字母数字与连字符，
// 不以连字符开头结尾，单标签 ≤ 63 字节，整体 ≤ 253 字节）。
func validProxyHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := 0; index < len(label); index++ {
			char := label[index]
			switch {
			case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '-':
			default:
				return false
			}
		}
	}
	return true
}

// validatePlainURL 校验不含占位符的普通 URL。
func validatePlainURL(field, rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("%s 不能为空", field)
	}
	if len(rawURL) > maxURLBytes {
		return fmt.Errorf("%s 不能超过 %d 字节", field, maxURLBytes)
	}
	if strings.ContainsAny(rawURL, "{}") {
		return fmt.Errorf("%s 不支持占位符", field)
	}
	return checkURLShape(field, rawURL)
}

func checkURLShape(field, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// url.Error 会把原始 URL 整个回显，而它可能带着内联的用户名密码，所以只报字段名。
		return fmt.Errorf("%s 不是合法 URL", field)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s 必须使用 http 或 https", field)
	}
	if parsed.Host == "" {
		return fmt.Errorf("%s 必须包含主机名", field)
	}
	if parsed.User != nil {
		return fmt.Errorf("%s 不允许内联用户名密码", field)
	}
	return nil
}

// validateModelName 限制模型名的字符集：它会被写进探针请求体，也会成为票池的键。
func validateModelName(field, model string) error {
	if model == "" {
		return fmt.Errorf("%s 不能为空", field)
	}
	if len(model) > MaxModelNameBytes {
		return fmt.Errorf("%s 不能超过 %d 字节", field, MaxModelNameBytes)
	}
	for index := 0; index < len(model); index++ {
		char := model[index]
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9':
		case char == '-' || char == '_' || char == '.' || char == ':' || char == '/':
		default:
			return fmt.Errorf("%s 含非法字符 %q，模型名只能包含字母、数字与 - _ . : /", field, string(char))
		}
	}
	return nil
}

// ValidModelName 判断一个模型名是否满足与配置项 models 相同的字符集与长度规则。
// 从请求体里嗅探到的模型名也按这套规则决定能不能为它建池。
func ValidModelName(model string) bool {
	return validateModelName("model", model) == nil
}

func validateHeaderMap(field string, headers map[string]string, scope headerScope) error {
	if len(headers) > maxExtraHeaderEntries {
		return fmt.Errorf("%s 最多 %d 条，当前 %d 条", field, maxExtraHeaderEntries, len(headers))
	}
	for name, value := range headers {
		if err := validateHeaderName(field+"."+name, name, scope); err != nil {
			return err
		}
		if len(value) > maxHeaderValueBytes {
			return fmt.Errorf("%s.%s 的值不能超过 %d 字节", field, name, maxHeaderValueBytes)
		}
		if containsControlCharacter(value) {
			return fmt.Errorf("%s.%s 的值不能包含控制字符", field, name)
		}
	}
	return nil
}

// validateHeaderName 校验 RFC 7230 token，并按作用域决定是否套用受保护头名单。
func validateHeaderName(field, name string, scope headerScope) error {
	if name == "" {
		return fmt.Errorf("%s 不能为空", field)
	}
	if len(name) > maxHeaderNameBytes {
		return fmt.Errorf("%s 不能超过 %d 字节", field, maxHeaderNameBytes)
	}
	for _, char := range []byte(name) {
		if !isTokenByte(char) {
			return fmt.Errorf("%s 包含非法字符 %q，必须是合法的 HTTP 头名称", field, string(char))
		}
	}
	lowered := strings.ToLower(name)
	if scope == scopeGuardHeader && lowered == strings.ToLower(DefaultHeaderName) {
		return nil
	}
	if _, blocked := headerOverrideBlockedNames[lowered]; blocked {
		return fmt.Errorf("%s 使用了受保护的头名称 %q", field, name)
	}
	return nil
}

func isTokenByte(char byte) bool {
	switch {
	case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", char) >= 0
}

// containsControlCharacter 与宿主 httpguts.ValidHeaderFieldValue 的判定对齐：
// 头值不允许控制字符（制表符除外）。
func containsControlCharacter(value string) bool {
	for _, char := range []byte(value) {
		if char == '\t' {
			continue
		}
		if char < 0x20 || char == 0x7f {
			return true
		}
	}
	return false
}

func checkRange(field string, value, minimum, maximum int) error {
	if value < minimum || value > maximum {
		return fmt.Errorf("%s 必须在 %d-%d 之间，当前 %d", field, minimum, maximum, value)
	}
	return nil
}
