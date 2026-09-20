package pluginconfig

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestParseEmptyYieldsDefaults(t *testing.T) {
	for _, raw := range []string{"", "   ", "{}", "\n{\n}\n"} {
		config, err := Parse([]byte(raw))
		if err != nil {
			t.Fatalf("Parse(%q) 出错: %v", raw, err)
		}
		guard := config.OverloadGuard
		if guard.HeaderName != DefaultHeaderName {
			t.Fatalf("Parse(%q) header_name = %q，期望 %q", raw, guard.HeaderName, DefaultHeaderName)
		}
		if guard.OverrideMode != OverrideModeAlways {
			t.Fatalf("Parse(%q) override_mode = %q，期望 %q", raw, guard.OverrideMode, OverrideModeAlways)
		}
		if guard.OnUnavailable != OnUnavailablePassthrough {
			t.Fatalf("Parse(%q) on_unavailable = %q", raw, guard.OnUnavailable)
		}
		if guard.OnModelUnmatched != OnUnavailablePassthrough {
			t.Fatalf("Parse(%q) on_model_unmatched = %q", raw, guard.OnModelUnmatched)
		}
		if guard.ModelSniffMaxBytes != DefaultModelSniffMaxBytes {
			t.Fatalf("Parse(%q) model_sniff_max_bytes = %d，期望 %d",
				raw, guard.ModelSniffMaxBytes, DefaultModelSniffMaxBytes)
		}
		pool := guard.TicketPool
		if pool.PoolSize != DefaultPoolSize {
			t.Fatalf("Parse(%q) 票池默认值不正确: %+v", raw, pool)
		}
		if pool.TicketTTLSeconds != DefaultTicketTTLSeconds || pool.RefillThresholdSeconds != DefaultRefillThresholdSeconds {
			t.Fatalf("Parse(%q) 票池时效默认值不正确: %+v", raw, pool)
		}
		if pool.GatewayBaseURL != DefaultGatewayBaseURL || pool.UserAgent != DefaultUserAgent {
			t.Fatalf("Parse(%q) 网关默认值不正确: %+v", raw, pool)
		}
		if len(pool.Models) != len(DefaultPoolModels()) {
			t.Fatalf("Parse(%q) 默认模型列表 = %v", raw, pool.Models)
		}
		proxies := guard.ProxyPool
		if !proxies.Enabled || len(proxies.Proxies) != 0 {
			t.Fatalf("Parse(%q) 代理库默认值不正确: %+v", raw, proxies)
		}
		if config.ProxyMode != ProxyModeHost || config.TLSMinVersion != "1.2" || !config.EnableHTTP2 {
			t.Fatalf("Parse(%q) 传输默认值不正确: %+v", raw, config)
		}
		if guard.Accounts == nil || config.ExtraHeaders == nil {
			t.Fatalf("Parse(%q) 规范化后 accounts/extra_headers 不应为 nil", raw)
		}
	}
}

// 代理地址的规范化：补协议、补默认端口、丢掉路径，且必须幂等。
func TestProxyItemNormalization(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"proxy_pool":{"proxies":[
		{"name":" hk-1 ","url":" 1.2.3.4:1080 "},
		{"url":"SOCKS5H://example.com"},
		{"url":"http://user:pass@10.0.0.9"},
		{"url":"https://10.0.0.10:8443/path?x=1#frag","enabled":false}
	]}}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	items := config.OverloadGuard.ProxyPool.Proxies
	want := []ProxyItem{
		{Name: "hk-1", URL: "socks5://1.2.3.4:1080", Enabled: true},
		{Name: "", URL: "socks5h://example.com:1080", Enabled: true},
		{Name: "", URL: "http://user:pass@10.0.0.9:80", Enabled: true},
		{Name: "", URL: "https://10.0.0.10:8443", Enabled: false},
	}
	if len(items) != len(want) {
		t.Fatalf("代理条数 = %d，期望 %d", len(items), len(want))
	}
	for index, expected := range want {
		if items[index] != expected {
			t.Fatalf("proxies[%d] = %+v，期望 %+v", index, items[index], expected)
		}
	}
	if scheme := items[1].ProxyScheme(); scheme != ProxySchemeSOCKS5H {
		t.Fatalf("ProxyScheme() = %q", scheme)
	}
}

// 代理池早期是「按接口拉取 socks5 列表」，那些键必须继续能解析（值被忽略），
// 否则装着旧配置的实例一升级就起不来。
func TestParseIgnoresRetiredProxyFields(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"proxy_pool":{
		"enabled":true,
		"sources":[{"name":"gen","url":"http://gen.example.com/g?c={count}","count":10,"mode":"batch"}],
		"refresh_interval_seconds":300,"test_url":"https://chatgpt.com/","test_timeout_seconds":10,
		"fetch_timeout_seconds":15,"ttl_seconds":86400,"max_use_fails":3,
		"max_consecutive_fails":3,"max_size":200}}}`))
	if err != nil {
		t.Fatalf("旧配置应当仍可解析: %v", err)
	}
	if len(config.OverloadGuard.ProxyPool.Proxies) != 0 {
		t.Fatalf("旧的 sources 不应变成代理条目: %+v", config.OverloadGuard.ProxyPool.Proxies)
	}
	raw, err := config.Marshal()
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	if strings.Contains(string(raw), "sources") || strings.Contains(string(raw), "max_size") {
		t.Fatalf("规范化结果里不该再有已移除的键:\n%s", raw)
	}
}

// 规范化结果必须能原样回灌，否则宿主保存-加载往返会失败。
func TestParseNormalizedRoundTrip(t *testing.T) {
	first, err := Parse([]byte(`{"overload_guard":{"ticket_pool":{"pool_size":3},
		"accounts":[{"account_id":7,"enabled":true}]}}`))
	if err != nil {
		t.Fatalf("首次 Parse 出错: %v", err)
	}
	raw, err := first.Marshal()
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	second, err := Parse(raw)
	if err != nil {
		t.Fatalf("回灌规范化配置出错: %v\n%s", err, raw)
	}
	again, err := second.Marshal()
	if err != nil {
		t.Fatalf("二次 Marshal 出错: %v", err)
	}
	if string(raw) != string(again) {
		t.Fatalf("规范化不是幂等的:\n%s\n%s", raw, again)
	}
}

func TestParseRejectsUnknownFieldAndTrailingJSON(t *testing.T) {
	cases := map[string]string{
		"顶层未知字段":           `{"unknown_top":1}`,
		"guard 未知字段":       `{"overload_guard":{"typo_enabled":true}}`,
		"ticket_pool 未知字段": `{"overload_guard":{"ticket_pool":{"poolsize":3}}}`,
		"proxy_pool 未知字段":  `{"overload_guard":{"proxy_pool":{"srcs":[]}}}`,
		"proxy 条目未知字段":     `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"1.2.3.4:1080","kind":"x"}]}}}`,
		"account 未知字段":     `{"overload_guard":{"accounts":[{"account_id":1,"label":"x"}]}}`,
		"多个 JSON 值":        `{} {}`,
		"非对象":              `[]`,
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: Parse(%s) 应当报错", name, raw)
		}
	}
}

func TestParseExplicitFalseAndZeroAreHonored(t *testing.T) {
	config, err := Parse([]byte(`{"enable_http2":false,"max_connections_per_host":0,"http2_read_idle_timeout_seconds":0,
		"overload_guard":{"enabled":false,"ticket_pool":{"include_direct":false,"follow_observed_models":false},
		"proxy_pool":{"enabled":false}}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	if config.EnableHTTP2 {
		t.Fatal("enable_http2 显式 false 被默认值覆盖了")
	}
	if config.MaxConnectionsPerHost != 0 {
		t.Fatalf("max_connections_per_host = %d，期望 0（不限制）", config.MaxConnectionsPerHost)
	}
	if config.HTTP2ReadIdleTimeoutSeconds != 0 {
		t.Fatalf("http2_read_idle_timeout_seconds = %d，期望 0", config.HTTP2ReadIdleTimeoutSeconds)
	}
	if config.OverloadGuard.Enabled {
		t.Fatal("overload_guard.enabled 显式 false 被默认值覆盖了")
	}
	if config.OverloadGuard.TicketPool.IncludeDirect || config.OverloadGuard.TicketPool.FollowObservedModels {
		t.Fatal("票池的布尔开关显式 false 被默认值覆盖了")
	}
	if config.OverloadGuard.ProxyPool.Enabled {
		t.Fatal("proxy_pool.enabled 显式 false 被默认值覆盖了")
	}
}

func TestTicketPoolValidation(t *testing.T) {
	for name, raw := range map[string]string{
		"pool_size 为 0":              `{"overload_guard":{"ticket_pool":{"pool_size":0}}}`,
		"pool_size 过大":               `{"overload_guard":{"ticket_pool":{"pool_size":21}}}`,
		"ticket_ttl 过短":              `{"overload_guard":{"ticket_pool":{"ticket_ttl_seconds":299}}}`,
		"refill_threshold 超过 ttl":    `{"overload_guard":{"ticket_pool":{"ticket_ttl_seconds":600,"refill_threshold_seconds":601}}}`,
		"check_interval 过短":          `{"overload_guard":{"ticket_pool":{"check_interval_seconds":9}}}`,
		"probe_timeout 过短":           `{"overload_guard":{"ticket_pool":{"probe_timeout_seconds":4}}}`,
		"probe_effort 非法":            `{"overload_guard":{"ticket_pool":{"probe_effort":"insane"}}}`,
		"retry_rounds 过大":            `{"overload_guard":{"ticket_pool":{"retry_rounds":11}}}`,
		"gateway_base_url 非 http":    `{"overload_guard":{"ticket_pool":{"gateway_base_url":"ftp://a.example.com"}}}`,
		"gateway_base_url 内联凭据":      `{"overload_guard":{"ticket_pool":{"gateway_base_url":"https://u:p@a.example.com"}}}`,
		"模型名含空格":                     `{"overload_guard":{"ticket_pool":{"models":["gpt 5"]}}}`,
		"模型列表为空且不跟踪":                 `{"overload_guard":{"ticket_pool":{"models":[],"follow_observed_models":false}}}`,
		"user_agent 含控制字符":           `{"overload_guard":{"ticket_pool":{"user_agent":"codex\u0001"}}}`,
		"ticket_stagger_seconds 超上限": `{"overload_guard":{"ticket_pool":{"ticket_stagger_seconds":3601}}}`,
		"proxies_per_round 超上限":      `{"overload_guard":{"ticket_pool":{"proxies_per_round":33}}}`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}

	// 模型列表去空、去重、保序。
	config, err := Parse([]byte(`{"overload_guard":{"ticket_pool":{"models":[" gpt-5.6-sol ","","gpt-5.6-sol","gpt-6-astra"]}}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	models := config.OverloadGuard.TicketPool.Models
	if len(models) != 2 || models[0] != "gpt-5.6-sol" || models[1] != "gpt-6-astra" {
		t.Fatalf("模型列表未按「去空去重保序」规范化: %v", models)
	}

	// 超过上限的模型数会把撞票流量放大到不可控，必须拒绝。
	entries := make([]string, 0, MaxPoolModels+1)
	for index := 0; index <= MaxPoolModels; index++ {
		entries = append(entries, fmt.Sprintf(`"m-%d"`, index))
	}
	raw := fmt.Sprintf(`{"overload_guard":{"ticket_pool":{"models":[%s]}}}`, strings.Join(entries, ","))
	if _, err := Parse([]byte(raw)); err == nil {
		t.Fatalf("models 超过 %d 个时应当被拒绝", MaxPoolModels)
	}
}

// 满血判定不再看 state 长度，曾经的两个键（全局的 target_state_length 与按模型的
// model_state_lengths）被移除。宿主里存着的旧配置还带着它们，而解析是
// DisallowUnknownFields 的——必须接受并忽略，否则升级后第一次 ApplyConfig 就整体失败。
func TestLegacyStateLengthKeysIgnored(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"ticket_pool":{"pool_size":3,
		"target_state_length":292,
		"model_state_lengths":{"gpt-5.6-luna":292,"gpt-6-astra":312}}}}`))
	if err != nil {
		t.Fatalf("带旧长度字段的配置应当能解析: %v", err)
	}
	if config.OverloadGuard.TicketPool.PoolSize != 3 {
		t.Fatalf("同一份配置里的其它字段应当照常生效: %+v", config.OverloadGuard.TicketPool)
	}
	// 回存给宿主的规范化结果里不该再出现这两个键，下一次保存它们就消失了。
	encoded, err := config.Marshal()
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	for _, key := range []string{"target_state_length", "model_state_lengths"} {
		if strings.Contains(string(encoded), key) {
			t.Fatalf("规范化结果里仍然有已移除的键 %s: %s", key, encoded)
		}
	}
	// 规范化必须幂等：TestConfig 靠比对两次 Marshal 判断配置是否生效。
	again, err := Parse(encoded)
	if err != nil {
		t.Fatalf("回灌出错: %v", err)
	}
	second, err := again.Marshal()
	if err != nil {
		t.Fatalf("二次 Marshal 出错: %v", err)
	}
	if string(encoded) != string(second) {
		t.Fatalf("规范化不幂等:\n%s\n%s", encoded, second)
	}
}

func TestProxyPoolValidation(t *testing.T) {
	for name, raw := range map[string]string{
		"地址为空":    `{"overload_guard":{"proxy_pool":{"proxies":[{"name":"a","url":"  "}]}}}`,
		"协议不支持":   `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks4://1.2.3.4:1080"}]}}}`,
		"主机为空":    `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5://:1080"}]}}}`,
		"端口非数字":   `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5://1.2.3.4:http"}]}}}`,
		"端口越界":    `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5://1.2.3.4:65536"}]}}}`,
		"地址含控制字符": fmt.Sprintf(`{"overload_guard":{"proxy_pool":{"proxies":[{"url":%s}]}}}`, ctrlJSON(t, "socks5://1.2.3.4:1080\x01")),
		"地址重复":    `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"1.2.3.4:1080"},{"url":"socks5://1.2.3.4:1080"}]}}}`,
		"备注含控制字符": fmt.Sprintf(`{"overload_guard":{"proxy_pool":{"proxies":[{"name":%s,"url":"1.2.3.4:1080"}]}}}`, ctrlJSON(t, "a\x01b")),
		"备注过长":    `{"overload_guard":{"proxy_pool":{"proxies":[{"name":"` + strings.Repeat("x", 41) + `","url":"1.2.3.4:1080"}]}}}`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}

	// 代理地址可能内联 user:pass，报错信息里绝不能回显原始 URL。
	_, err := Parse([]byte(`{"overload_guard":{"proxy_pool":{"proxies":[{"name":"hk","url":"socks4://u:s3cret@1.2.3.4:1080"}]}}}`))
	if err == nil {
		t.Fatal("非法协议应当被拒绝")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("错误信息泄露了代理凭据: %v", err)
	}

	// 条目数上限。
	entries := make([]string, 0, MaxProxyItems+1)
	for index := 0; index <= MaxProxyItems; index++ {
		entries = append(entries, fmt.Sprintf(`{"url":"socks5://10.0.0.1:%d"}`, 10000+index))
	}
	raw := fmt.Sprintf(`{"overload_guard":{"proxy_pool":{"proxies":[%s]}}}`, strings.Join(entries, ","))
	if _, err := Parse([]byte(raw)); err == nil {
		t.Fatalf("proxy_pool.proxies 超过 %d 条时应当被拒绝", MaxProxyItems)
	}
}

// 既不直连又没有任何代理，撞票永远发不出去：这是配置错误，应当在保存时就拦下来。
func TestGuardRequiresSomeCaptureOutlet(t *testing.T) {
	raw := `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
		"ticket_pool":{"include_direct":false},"proxy_pool":{"enabled":false}}}`
	err := parseErr(t, raw)
	if err == nil {
		t.Fatal("应当被拒绝：没有任何撞票出口")
	}
	if !strings.Contains(err.Error(), "include_direct") {
		t.Fatalf("错误信息应当点明缺的是什么: %v", err)
	}

	// 代理库开着但一条都没填，同样没有出口——默认配置就是这样，所以这条必须也被拦下。
	if err := parseErr(t, `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
		"ticket_pool":{"include_direct":false}}}`); err == nil {
		t.Fatal("应当被拒绝：代理库里没有任何启用的代理")
	}
	// 代理都停用了也一样。
	if err := parseErr(t, `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
		"ticket_pool":{"include_direct":false},
		"proxy_pool":{"proxies":[{"url":"1.2.3.4:1080","enabled":false}]}}}`); err == nil {
		t.Fatal("应当被拒绝：唯一的代理是停用的")
	}
	// 代理都在、直连关着，但每轮一条代理都不用，同样发不出去。
	if err := parseErr(t, `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
		"ticket_pool":{"include_direct":false,"proxies_per_round":0},
		"proxy_pool":{"proxies":[{"url":"1.2.3.4:1080"}]}}}`); err == nil || !strings.Contains(err.Error(), "proxies_per_round") {
		t.Fatalf("应当被拒绝并点明 proxies_per_round: %v", err)
	}

	// 只要还留着直连，或者还留着一条启用的代理，就都是合法配置。
	for name, ok := range map[string]string{
		"保留直连": `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
			"ticket_pool":{"include_direct":true},"proxy_pool":{"enabled":false}}}`,
		"保留代理": `{"overload_guard":{"accounts":[{"account_id":7,"enabled":true}],
			"ticket_pool":{"include_direct":false},
			"proxy_pool":{"proxies":[{"name":"hk","url":"1.2.3.4:1080"}]}}}`,
		"没有启用的账号": `{"overload_guard":{"accounts":[{"account_id":7,"enabled":false}],
			"ticket_pool":{"include_direct":false},"proxy_pool":{"enabled":false}}}`,
		"总开关关闭": `{"overload_guard":{"enabled":false,"accounts":[{"account_id":7,"enabled":true}],
			"ticket_pool":{"include_direct":false},"proxy_pool":{"enabled":false}}}`,
	} {
		if err := parseErr(t, ok); err != nil {
			t.Fatalf("%s: 不应被拒绝: %v", name, err)
		}
	}
}

// parseErr 只取 Parse 的错误，便于在表驱动用例里做断言。
func parseErr(t *testing.T, raw string) error {
	t.Helper()
	_, err := Parse([]byte(raw))
	return err
}

// ctrlJSON 把带控制字符的值编成合法的 JSON 字符串字面量（控制字符会被转成 JSON 转义序列），
// 这样用例里不必在 Go 源码中塞裸控制字符，JSON 本身也仍然是合法的——
// 被拒绝的原因才会落在校验逻辑上，而不是 JSON 解码。
func ctrlJSON(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("编码控制字符用例出错: %v", err)
	}
	return string(encoded)
}

// 主机必须是 IP 或域名：url.Parse 太宽松，没加方括号的 IPv6 和「协议写成前缀」的地址都会被它
// 拆成怪异的主机 + 端口，这些要在保存时拦下，而不是等撞票时报拨号错误。
func TestProxyHostRules(t *testing.T) {
	for name, raw := range map[string]string{
		"IPv6 没加方括号": `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5://2001:db8::1"}]}}}`,
		"协议写成前缀":     `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5:1.2.3.4:1080"}]}}}`,
		"域名标签以连字符开头": `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"socks5://-bad.example.com:1080"}]}}}`,
		"主机含下划线":     `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"http://my_proxy:8080"}]}}}`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}
	for name, raw := range map[string]string{
		"方括号 IPv6": `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"[2001:db8::1]:1080"}]}}}`,
		"域名":       `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"https://proxy.example.com"}]}}}`,
		"IPv4":     `{"overload_guard":{"proxy_pool":{"proxies":[{"url":"10.0.0.1:1080"}]}}}`,
	} {
		if _, err := Parse([]byte(raw)); err != nil {
			t.Fatalf("%s: 应当被接受，实际 %v", name, err)
		}
	}
}

func TestAccountValidation(t *testing.T) {
	cases := map[string]string{
		"account_id 为 0": `{"overload_guard":{"accounts":[{"account_id":0}]}}`,
		"account_id 为负":  `{"overload_guard":{"accounts":[{"account_id":-3}]}}`,
		"account_id 重复":  `{"overload_guard":{"accounts":[{"account_id":7},{"account_id":7}]}}`,
		"note 含控制字符":     `{"overload_guard":{"accounts":[{"account_id":7,"note":"a\u0001b"}]}}`,
		"note 超长": fmt.Sprintf(`{"overload_guard":{"accounts":[{"account_id":7,"note":%q}]}}`,
			strings.Repeat("x", maxAccountNoteBytes+1)),
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}

	// 账号条目只剩开关与备注：不再需要任何外部服务端信息。
	if _, err := Parse([]byte(`{"overload_guard":{"accounts":[{"account_id":7,"enabled":true,"note":"主力号"}]}}`)); err != nil {
		t.Fatalf("最简账号条目应当通过: %v", err)
	}

	var entries []string
	for index := 0; index <= MaxAccounts; index++ {
		entries = append(entries, fmt.Sprintf(`{"account_id":%d,"enabled":false}`, index+1))
	}
	raw := fmt.Sprintf(`{"overload_guard":{"accounts":[%s]}}`, strings.Join(entries, ","))
	if _, err := Parse([]byte(raw)); err == nil {
		t.Fatalf("accounts 超过 %d 条时应当被拒绝", MaxAccounts)
	}
}

func TestEnabledAccountsAndLookup(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"accounts":[
		{"account_id":3,"enabled":true},
		{"account_id":2,"enabled":false},
		{"account_id":1,"enabled":true,"note":"主力号"}
	]}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	accounts := config.EnabledAccounts()
	if len(accounts) != 2 || accounts[0].AccountID != 1 || accounts[1].AccountID != 3 {
		t.Fatalf("EnabledAccounts 应当按 ID 升序返回启用项: %+v", accounts)
	}
	if accounts[0].Note != "主力号" {
		t.Fatalf("备注丢失: %+v", accounts[0])
	}
	if !config.AccountEnabled(1) || config.AccountEnabled(2) || config.AccountEnabled(404) {
		t.Fatal("AccountEnabled 判定不正确")
	}

	// 总开关关闭时，任何账号都不被接管。
	config.OverloadGuard.Enabled = false
	if config.AccountEnabled(1) || len(config.EnabledAccounts()) != 0 {
		t.Fatal("总开关关闭后不应有任何账号被接管")
	}
}

func TestHeaderNameRules(t *testing.T) {
	// 目标头本身就是宿主黑名单里的 x-codex-turn-state，必须放行。
	if _, err := Parse([]byte(`{"overload_guard":{"header_name":"x-codex-turn-state"}}`)); err != nil {
		t.Fatalf("header_name = x-codex-turn-state 应当放行: %v", err)
	}
	for name, raw := range map[string]string{
		"注入受保护头 Authorization": `{"overload_guard":{"header_name":"Authorization"}}`,
		"注入受保护头 Host":          `{"overload_guard":{"header_name":"Host"}}`,
		"非 token 字符":           `{"overload_guard":{"header_name":"X Codex State"}}`,
		"带冒号":                  `{"overload_guard":{"header_name":"X-Codex:State"}}`,
		"空字符串":                 `{"overload_guard":{"header_name":"   "}}`,
	} {
		if name == "空字符串" {
			// 空字符串会被规范化为默认头名，属于合法输入。
			config, err := Parse([]byte(raw))
			if err != nil || config.OverloadGuard.HeaderName != DefaultHeaderName {
				t.Fatalf("空 header_name 应当规范化为默认值，得到 err=%v name=%q",
					err, config.OverloadGuard.HeaderName)
			}
			continue
		}
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}
}

func TestExtraHeadersRules(t *testing.T) {
	// 上游额外头受宿主黑名单约束。
	if _, err := Parse([]byte(`{"extra_headers":{"Authorization":"Bearer x"}}`)); err == nil {
		t.Fatal("上游 extra_headers 不应允许 Authorization")
	}
	if _, err := Parse([]byte(`{"extra_headers":{"X-Trace":"a\u0001b"}}`)); err == nil {
		t.Fatal("头值含控制字符应当被拒绝")
	}
	if _, err := Parse([]byte(fmt.Sprintf(`{"extra_headers":{"X-Big":%q}}`,
		strings.Repeat("a", maxHeaderValueBytes+1)))); err == nil {
		t.Fatal("超长头值应当被拒绝")
	}
	entries := make([]string, 0, maxExtraHeaderEntries+1)
	for index := 0; index <= maxExtraHeaderEntries; index++ {
		entries = append(entries, fmt.Sprintf(`"X-H%d":"v"`, index))
	}
	if _, err := Parse([]byte(fmt.Sprintf(`{"extra_headers":{%s}}`, strings.Join(entries, ",")))); err == nil {
		t.Fatalf("extra_headers 超过 %d 条应当被拒绝", maxExtraHeaderEntries)
	}
}

// 移除 Provider 之后，旧配置里那些键必须「能解析、不采纳」：宿主存着的是上一版配置，
// 而解析是 DisallowUnknownFields 的——直接删字段会让升级后第一次 ApplyConfig 整体失败。
func TestRemovedFieldsAreAcceptedAndDropped(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{
		"model_aliases":{"gpt-5-codex-preview":"gpt-5-codex"},
		"default_provider":{"url":"https://guard.example.com/v1/state","api_key":"secret",
			"method":"GET","models_field":"tickets","refresh_interval_seconds":600},
		"accounts":[{"account_id":1,"enabled":true,"provider_account_id":"acc_1",
			"provider":{"url":"https://one.example.com/state","api_key":"another-secret"}}]}}`))
	if err != nil {
		t.Fatalf("含已移除字段的旧配置应当仍可解析: %v", err)
	}
	if len(config.EnabledAccounts()) != 1 {
		t.Fatalf("账号本身应当被保留: %+v", config.OverloadGuard.Accounts)
	}
	raw, err := config.Marshal()
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	for _, key := range []string{
		"model_aliases", "default_provider", "provider_account_id", "models_field",
		"api_key", "secret", "another-secret",
	} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("规范化输出仍包含已移除内容 %q:\n%s", key, raw)
		}
	}
	// 回灌一次，确认清掉这些键之后配置仍然自洽。
	if _, err := Parse(raw); err != nil {
		t.Fatalf("清理后的配置应当仍可解析: %v", err)
	}
}

// on_model_unmatched 与 on_unavailable 是两套独立策略：前者是「有票但没有这个模型的」，
// 后者是「一张票都还没撞到」。
func TestOnModelUnmatchedValidation(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"on_model_unmatched":" FAIL_REQUEST "}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	if config.OverloadGuard.OnModelUnmatched != OnUnavailableFailRequest {
		t.Fatalf("on_model_unmatched = %q，期望规范化为 fail_request", config.OverloadGuard.OnModelUnmatched)
	}
	if config.OverloadGuard.OnUnavailable != OnUnavailablePassthrough {
		t.Fatal("on_model_unmatched 不应影响 on_unavailable")
	}
	if _, err := Parse([]byte(`{"overload_guard":{"on_model_unmatched":"borrow_any"}}`)); err == nil {
		t.Fatal("非法的 on_model_unmatched 应当被拒绝：跨模型借用 state 不是合法取值")
	}
}

func TestModelSniffMaxBytesBoundaries(t *testing.T) {
	cases := []struct {
		bytes int
		ok    bool
	}{
		{MinModelSniffMaxBytes - 1, false},
		{MinModelSniffMaxBytes, true},
		{DefaultModelSniffMaxBytes, true},
		{MaxModelSniffMaxBytes, true},
		{MaxModelSniffMaxBytes + 1, false},
		{-1, false},
	}
	for _, tc := range cases {
		raw := fmt.Sprintf(`{"overload_guard":{"model_sniff_max_bytes":%d}}`, tc.bytes)
		config, err := Parse([]byte(raw))
		if tc.ok {
			if err != nil {
				t.Fatalf("model_sniff_max_bytes=%d 应当通过，却报错: %v", tc.bytes, err)
			}
			if config.OverloadGuard.SniffLimit() != tc.bytes {
				t.Fatalf("SniffLimit() = %d，期望 %d", config.OverloadGuard.SniffLimit(), tc.bytes)
			}
			continue
		}
		if err == nil {
			t.Fatalf("model_sniff_max_bytes=%d 应当被拒绝", tc.bytes)
		}
	}
	// 0 表示"未填写"，规范化为默认值而不是报错。
	config, err := Parse([]byte(`{"overload_guard":{"model_sniff_max_bytes":0}}`))
	if err != nil {
		t.Fatalf("model_sniff_max_bytes=0 应当规范化为默认值: %v", err)
	}
	if config.OverloadGuard.ModelSniffMaxBytes != DefaultModelSniffMaxBytes {
		t.Fatalf("model_sniff_max_bytes=0 规范化为 %d", config.OverloadGuard.ModelSniffMaxBytes)
	}
	// 配置结构被绕过（例如内部构造零值）时也要有兜底。
	if got := (Guard{}).SniffLimit(); got != DefaultModelSniffMaxBytes {
		t.Fatalf("零值 Guard 的 SniffLimit() = %d，期望回落到默认值", got)
	}
}

func TestTransportValidation(t *testing.T) {
	for name, raw := range map[string]string{
		"整体超时过小":                `{"request_timeout_seconds":30}`,
		"响应头超时为 0":              `{"response_header_timeout_seconds":0}`,
		"per-host 空闲连接超过总数":     `{"max_idle_connections":10,"max_idle_connections_per_host":11}`,
		"总连接数小于 per-host 空闲连接数": `{"max_idle_connections_per_host":120,"max_connections_per_host":10}`,
		"TLS 版本不支持":             `{"tls_min_version":"1.1"}`,
		"代理模式非法":                `{"proxy_mode":"always"}`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("%s: 应当被拒绝", name)
		}
	}
	// 0 表示不限制整体超时，是流式响应的推荐值。
	if _, err := Parse([]byte(`{"request_timeout_seconds":0}`)); err != nil {
		t.Fatalf("request_timeout_seconds=0 应当放行: %v", err)
	}
	if _, err := Parse([]byte(`{"proxy_mode":" Disabled "}`)); err != nil {
		t.Fatalf("proxy_mode 应当大小写/空白不敏感: %v", err)
	}
}

func TestCloneIsolatesMapsAndSlices(t *testing.T) {
	config, err := Parse([]byte(`{"extra_headers":{"X-Trace":"a"},"overload_guard":{
		"ticket_pool":{"models":["gpt-5.6-sol"]},
		"proxy_pool":{"proxies":[{"name":"hk","url":"socks5://1.2.3.4:1080"}]},
		"accounts":[{"account_id":1,"enabled":true}]}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}

	clone := config.Clone()
	clone.ExtraHeaders["X-Trace"] = "mutated"
	clone.OverloadGuard.TicketPool.Models[0] = "mutated"
	clone.OverloadGuard.ProxyPool.Proxies[0].URL = "socks5://9.9.9.9:1080"
	clone.OverloadGuard.Accounts[0].Note = "mutated"
	clone.OverloadGuard.Accounts = append(clone.OverloadGuard.Accounts, Account{AccountID: 2})

	if config.ExtraHeaders["X-Trace"] != "a" {
		t.Fatal("Clone 与原配置共享 extra_headers")
	}
	if config.OverloadGuard.TicketPool.Models[0] != "gpt-5.6-sol" {
		t.Fatal("Clone 与原配置共享 models 切片")
	}
	if config.OverloadGuard.ProxyPool.Proxies[0].URL != "socks5://1.2.3.4:1080" {
		t.Fatal("Clone 与原配置共享 proxies 切片")
	}
	if config.OverloadGuard.Accounts[0].Note != "" || len(config.OverloadGuard.Accounts) != 1 {
		t.Fatal("Clone 与原配置共享 accounts 切片")
	}
}

func TestActiveProxies(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"proxy_pool":{"proxies":[
		{"name":"on","url":"socks5://1.2.3.4:1080","enabled":true},
		{"name":"off","url":"http://5.6.7.8:8080","enabled":false}]}}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	active := config.OverloadGuard.ProxyPool.ActiveProxies()
	if len(active) != 1 || active[0].Name != "on" {
		t.Fatalf("ActiveProxies = %+v", active)
	}

	// 代理库总开关关闭时，逐条的 enabled 一律不作数。
	config.OverloadGuard.ProxyPool.Enabled = false
	if len(config.OverloadGuard.ProxyPool.ActiveProxies()) != 0 {
		t.Fatal("代理库关闭后不应有任何启用的代理")
	}
}

// 规范化输出必须包含所有字段，UI 才能无条件读取。
func TestMarshalContainsAllFields(t *testing.T) {
	config, err := Parse([]byte(`{"overload_guard":{"accounts":[{"account_id":7}],
		"proxy_pool":{"proxies":[{"name":"hk","url":"1.2.3.4:1080"}]}}}`))
	if err != nil {
		t.Fatalf("Parse 出错: %v", err)
	}
	raw, err := config.Marshal()
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal 出错: %v", err)
	}
	for _, key := range []string{
		"request_timeout_seconds", "response_header_timeout_seconds", "dial_timeout_seconds",
		"tls_handshake_timeout_seconds", "idle_connection_timeout_seconds", "max_idle_connections",
		"max_idle_connections_per_host", "max_connections_per_host", "enable_http2",
		"http2_read_idle_timeout_seconds", "tls_min_version", "proxy_mode", "extra_headers", "overload_guard",
	} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("规范化输出缺少字段 %q", key)
		}
	}

	var guard map[string]json.RawMessage
	if err := json.Unmarshal(decoded["overload_guard"], &guard); err != nil {
		t.Fatalf("overload_guard 不是对象: %v", err)
	}
	for _, key := range []string{
		"enabled", "header_name", "override_mode", "on_unavailable", "on_model_unmatched",
		"model_sniff_max_bytes", "ticket_pool", "proxy_pool", "accounts",
	} {
		if _, ok := guard[key]; !ok {
			t.Fatalf("overload_guard 缺少字段 %q", key)
		}
	}

	var pool map[string]json.RawMessage
	if err := json.Unmarshal(guard["ticket_pool"], &pool); err != nil {
		t.Fatalf("ticket_pool 不是对象: %v", err)
	}
	for _, key := range []string{
		"models", "follow_observed_models", "pool_size", "ticket_ttl_seconds",
		"ticket_stagger_seconds", "refill_threshold_seconds", "check_interval_seconds",
		"probe_timeout_seconds", "probe_effort", "proxies_per_round", "retry_rounds",
		"include_direct", "gateway_base_url", "user_agent",
	} {
		if _, ok := pool[key]; !ok {
			t.Fatalf("ticket_pool 缺少字段 %q", key)
		}
	}

	var proxies map[string]json.RawMessage
	if err := json.Unmarshal(guard["proxy_pool"], &proxies); err != nil {
		t.Fatalf("proxy_pool 不是对象: %v", err)
	}
	for _, key := range []string{"enabled", "proxies"} {
		if _, ok := proxies[key]; !ok {
			t.Fatalf("proxy_pool 缺少字段 %q", key)
		}
	}

	var items []map[string]json.RawMessage
	if err := json.Unmarshal(proxies["proxies"], &items); err != nil {
		t.Fatalf("proxies 不是数组: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("用例里填了一条代理，规范化输出却是空的")
	}
	for _, key := range []string{"name", "url", "enabled"} {
		if _, ok := items[0][key]; !ok {
			t.Fatalf("代理条目缺少字段 %q", key)
		}
	}

	// 账号条目同样要字段齐全：UI 直接按键读，缺字段就会显示成 undefined。
	var full struct {
		OverloadGuard struct {
			Accounts []map[string]json.RawMessage `json:"accounts"`
		} `json:"overload_guard"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatalf("Unmarshal 出错: %v", err)
	}
	if len(full.OverloadGuard.Accounts) != 1 {
		t.Fatalf("accounts 条数 = %d", len(full.OverloadGuard.Accounts))
	}
	for _, key := range []string{"account_id", "enabled", "note"} {
		if _, ok := full.OverloadGuard.Accounts[0][key]; !ok {
			t.Fatalf("账号条目缺少字段 %q", key)
		}
	}
}
