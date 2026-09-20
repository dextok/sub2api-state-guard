package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
	"github.com/dextok/sub2api-state-guard/internal/proxypool"
)

const (
	testAccountID = int64(42)
	testModel     = "gpt-5-codex"
	// testBearer 是「从过路请求里采到的」假 OAuth 令牌。
	// 必须足够长：isUsableBearer 会挡掉 20 字节以下的值。
	testBearer = "Bearer test-access-token-0123456789abcdef"
)

func testIdentity() Identity {
	return Identity{PluginID: "sub2api.plugin.overload-guard", PluginVersion: "0.1.0"}
}

// gatewayStub 是 codex 网关的假桩：回一个指定长度的 x-codex-turn-state 头，
// 外加一段最小可用的 SSE，让撞票探针能判出「有产出」。
type gatewayStub struct {
	server *httptest.Server
	calls  atomic.Int64
	// length 是要回的 state 长度；等于该模型的满血长度才算满血票。
	length atomic.Int64
	status atomic.Int64
	// served 非空时，SSE 里自报这个模型名，用来模拟上游换模型服务（降级）。
	served atomic.Value
}

func newGatewayStub(t *testing.T) *gatewayStub {
	t.Helper()
	stub := &gatewayStub{}
	stub.length.Store(int64(pluginconfig.DefaultTargetStateLength))
	stub.status.Store(http.StatusOK)
	stub.served.Store("")
	stub.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		serial := stub.calls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		if length := int(stub.length.Load()); length > 0 {
			writer.Header().Set(pluginconfig.DefaultHeaderName, mintState(serial, length))
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(int(stub.status.Load()))
		if served, _ := stub.served.Load().(string); served != "" {
			raw, _ := json.Marshal(served)
			_, _ = fmt.Fprintf(writer,
				"event: response.created\ndata: {\"response\":{\"id\":\"r1\",\"model\":%s}}\n\n", raw)
		}
		_, _ = io.WriteString(writer, "event: response.output_text.delta\ndata: {\"delta\":\"ok\"}\n\n")
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

// base 返回网关根，形如 http://127.0.0.1:PORT/backend-api/codex。
func (g *gatewayStub) base() string { return g.server.URL + "/backend-api/codex" }

// mintState 造一张长度精确的假票。每次内容都不同：StoreRound 按 state 去重，
// 内容相同的票只会入池一次。
func mintState(serial int64, length int) string {
	prefix := fmt.Sprintf("state-%d-", serial)
	if len(prefix) >= length {
		return prefix[:length]
	}
	return prefix + strings.Repeat("x", length-len(prefix))
}

// guardJSON 构造一份「接管单个账号、撞票打到给定网关、代理库关闭」的配置。
//
// 代理库关掉是为了让撞票只走直连，测试里不必准备任何代理出口。
// extra 被插进 overload_guard 对象最前面，每条自带尾逗号。
func guardJSON(gatewayBase string, extra ...string) []byte {
	return fmt.Appendf(nil, `{
		"overload_guard": {
			%s
			"enabled": true,
			"accounts": [{"account_id": %d, "enabled": true, "note": "主力号"}],
			"ticket_pool": {
				"models": [%q],
				"follow_observed_models": false,
				"pool_size": 1,
				"check_interval_seconds": 3600,
				"probe_timeout_seconds": 5,
				"gateway_base_url": %q
			},
			"proxy_pool": {"enabled": false}
		}
	}`, strings.Join(extra, "\n"), testAccountID, testModel, gatewayBase)
}

func newServer(t *testing.T) *Server {
	t.Helper()
	server := New(testIdentity(), func(string, ...any) {})
	t.Cleanup(server.Close)
	return server
}

// newGuardedServer 一步搭好「服务 + 假网关 + 已生效配置」。
func newGuardedServer(t *testing.T, extra ...string) (*Server, *gatewayStub) {
	t.Helper()
	server := newServer(t)
	gateway := newGatewayStub(t)
	applyConfig(t, server, guardJSON(gateway.base(), extra...))
	return server, gateway
}

// applyConfig 走宿主的真实调用顺序：先 ValidateConfig 再把规范化结果交给 ApplyConfig。
func applyConfig(t *testing.T, server *Server, raw []byte) {
	t.Helper()
	validation, err := server.ValidateConfig(context.Background(), &pluginv1.ValidateConfigRequest{ConfigJson: raw})
	if err != nil {
		t.Fatalf("ValidateConfig 出错: %v", err)
	}
	if !validation.GetValid() {
		t.Fatalf("配置被拒绝: %s", validation.GetMessage())
	}
	applied, err := server.ApplyConfig(context.Background(), &pluginv1.ApplyConfigRequest{
		ConfigJson: validation.GetNormalizedConfigJson(),
	})
	if err != nil {
		t.Fatalf("ApplyConfig 出错: %v", err)
	}
	if !applied.GetApplied() {
		t.Fatalf("配置未被应用: %s", applied.GetMessage())
	}
}

// harvest 模拟「一个带 OAuth 令牌的真实请求经过插件」，让账号拿到撞票凭据。
func harvest(server *Server, gateway *gatewayStub) {
	header := http.Header{}
	header.Set("Authorization", testBearer)
	header.Set("ChatGpt-Account-Id", "acct-9")
	header.Set("User-Agent", "codex_cli_rs/test")
	server.state().tickets.Harvest(testAccountID, header, gateway.base()+"/responses")
}

// warmPool 走完整路径把池预热到可注入：先采凭据，再同步撞一轮票。
//
// 不等后台循环：它有 5 秒对账间隔外加 5 秒启动抖动，测试等不起，
// 而且那样会让用例依赖时序。返回撞到的那张票的值。
func warmPool(t *testing.T, server *Server, gateway *gatewayStub) string {
	t.Helper()
	harvest(server, gateway)
	if added := server.state().tickets.Refill(context.Background(), testAccountID, testModel); added != 1 {
		t.Fatalf("同步撞票新增 %d 张，期望 1 张", added)
	}
	value, ok := server.state().tickets.Value(testAccountID, testModel)
	if !ok {
		t.Fatal("已经撞到票，取值却失败")
	}
	return value
}

// splitDashboard 把 TestConfig 的 message 拆成给人看的部分与看板 JSON。
func splitDashboard(t *testing.T, message string) (string, dashboard) {
	t.Helper()
	index := strings.Index(message, StatusSentinel)
	if index < 0 {
		t.Fatalf("消息里没有看板数据: %q", message)
	}
	var payload dashboard
	if err := json.Unmarshal([]byte(message[index+len(StatusSentinel):]), &payload); err != nil {
		t.Fatalf("看板数据不是合法 JSON: %v", err)
	}
	return strings.TrimRight(message[:index], "\n"), payload
}

func TestGetInfoMatchesManifestIdentity(t *testing.T) {
	server := newServer(t)
	info, err := server.GetInfo(context.Background(), &pluginv1.GetInfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo 出错: %v", err)
	}
	// 宿主会逐字比对这四个字段与清单，任何偏差都会导致进程被杀。
	if info.GetPluginId() != testIdentity().PluginID || info.GetPluginVersion() != testIdentity().PluginVersion {
		t.Fatalf("身份不匹配: %s@%s", info.GetPluginId(), info.GetPluginVersion())
	}
	if info.GetProtocolVersion() != pluginv1.ProtocolVersion || info.GetTransportApiVersion() != pluginv1.TransportAPIVersion {
		t.Fatalf("协议版本不匹配: %d/%d", info.GetProtocolVersion(), info.GetTransportApiVersion())
	}
	if len(info.GetCapabilities()) != 1 || info.GetCapabilities()[0] != Capability {
		t.Fatalf("能力声明不匹配: %v", info.GetCapabilities())
	}
}

func TestHealthReportsTicketWaterline(t *testing.T) {
	server := newServer(t)
	health, err := server.Health(context.Background(), &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health 出错: %v", err)
	}
	if !health.GetHealthy() || !strings.Contains(health.GetMessage(), "还没有账号被接管") {
		t.Fatalf("初始健康信息 = %q", health.GetMessage())
	}

	gateway := newGatewayStub(t)
	applyConfig(t, server, guardJSON(gateway.base()))
	value := warmPool(t, server, gateway)

	health, err = server.Health(context.Background(), &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health 出错: %v", err)
	}
	message := health.GetMessage()
	if !health.GetHealthy() || !strings.Contains(message, "接管 1 个账号（1 个已取到凭据）") {
		t.Fatalf("健康信息 = %q", message)
	}
	if !strings.Contains(message, "票池 1/1 张") {
		t.Fatalf("健康信息应当报出票池水位: %q", message)
	}
	// 票与 OAuth 令牌都是凭据级的东西，任何对外输出里都不能出现。
	if strings.Contains(message, value) || strings.Contains(message, testBearer) {
		t.Fatalf("健康信息泄露了票或令牌: %q", message)
	}
}

func TestHealthWhenGuardDisabled(t *testing.T) {
	server := newServer(t)
	applyConfig(t, server, []byte(`{"overload_guard":{"enabled":false}}`))
	health, err := server.Health(context.Background(), &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health 出错: %v", err)
	}
	if !health.GetHealthy() || !strings.Contains(health.GetMessage(), "仅透传") {
		t.Fatalf("健康信息 = %q", health.GetMessage())
	}
}

func TestValidateConfigNormalizesEmptyObject(t *testing.T) {
	server := newServer(t)
	validation, err := server.ValidateConfig(context.Background(), &pluginv1.ValidateConfigRequest{ConfigJson: []byte(`{}`)})
	if err != nil {
		t.Fatalf("ValidateConfig 出错: %v", err)
	}
	if !validation.GetValid() {
		t.Fatalf("空对象应当合法: %s", validation.GetMessage())
	}

	// 宿主要求规范化结果是一个非空 JSON 对象，并且会原样保存。
	var normalized map[string]any
	if err := json.Unmarshal(validation.GetNormalizedConfigJson(), &normalized); err != nil {
		t.Fatalf("规范化结果不是 JSON 对象: %v", err)
	}
	if len(normalized) == 0 {
		t.Fatal("规范化结果不能是空对象")
	}
	parsed, err := pluginconfig.Parse(validation.GetNormalizedConfigJson())
	if err != nil {
		t.Fatalf("规范化结果无法被自己解析: %v", err)
	}
	if parsed.OverloadGuard.HeaderName != pluginconfig.DefaultHeaderName {
		t.Fatalf("默认头名称 = %q", parsed.OverloadGuard.HeaderName)
	}
	pool := parsed.OverloadGuard.TicketPool
	if pool.TargetStateLength != pluginconfig.DefaultTargetStateLength || pool.PoolSize != pluginconfig.DefaultPoolSize {
		t.Fatalf("票池默认值 = 长度 %d、容量 %d", pool.TargetStateLength, pool.PoolSize)
	}
	if len(pool.Models) == 0 {
		t.Fatal("默认应当预置一批要维护票池的模型")
	}
	// 代理库出厂是空的：代理由管理员自己填，插件不预置任何第三方出口。
	proxies := parsed.OverloadGuard.ProxyPool
	if !proxies.Enabled || len(proxies.Proxies) != 0 {
		t.Fatalf("代理库默认值 = %+v", proxies)
	}
}

func TestValidateConfigRejectsBadConfig(t *testing.T) {
	server := newServer(t)
	cases := []struct {
		name string
		raw  string
	}{
		{"未知字段", `{"unknown_field": 1}`},
		{"检查过于频繁", `{"overload_guard":{"ticket_pool":{"check_interval_seconds":5}}}`},
		{"池容量过大", `{"overload_guard":{"ticket_pool":{"pool_size":99}}}`},
		{"网关地址非法", `{"overload_guard":{"ticket_pool":{"gateway_base_url":"ftp://x/y"}}}`},
		{"代理协议不支持", `{"overload_guard":{"proxy_pool":{"proxies":[` +
			`{"name":"a","url":"socks4://1.2.3.4:1080"}]}}}`},
		{"没有任何撞票出口", `{"overload_guard":{"enabled":true,` +
			`"accounts":[{"account_id":1,"enabled":true}],` +
			`"ticket_pool":{"include_direct":false},"proxy_pool":{"enabled":false}}}`},
		{"非法 JSON", `{`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			validation, err := server.ValidateConfig(context.Background(),
				&pluginv1.ValidateConfigRequest{ConfigJson: []byte(testCase.raw)})
			if err != nil {
				t.Fatalf("校验失败不应返回 gRPC 错误: %v", err)
			}
			if validation.GetValid() {
				t.Fatal("配置应当被拒绝")
			}
			if strings.TrimSpace(validation.GetMessage()) == "" {
				t.Fatal("拒绝时必须给出可展示的原因")
			}
		})
	}
}

func TestApplyConfigKeepsPreviousConfigOnFailure(t *testing.T) {
	server, _ := newGuardedServer(t)
	before := server.state()

	applied, err := server.ApplyConfig(context.Background(),
		&pluginv1.ApplyConfigRequest{ConfigJson: []byte(`{"overload_guard":{"header_name":"Authorization"}}`)})
	if err != nil {
		t.Fatalf("应用失败不应返回 gRPC 错误: %v", err)
	}
	if applied.GetApplied() {
		t.Fatal("非法配置不应被应用")
	}
	if server.state() != before {
		t.Fatal("应用失败后必须保留旧状态")
	}
	if !server.state().tickets.Enabled(testAccountID) {
		t.Fatal("旧配置的账号应当仍被接管")
	}
}

// 插件没有可写目录，票与凭据只在内存里。反复保存配置会重建整套运行时对象，
// 但它们记在跨配置存活的 Registry 上，不能因为一次保存就被清空重撞。
func TestApplyConfigKeepsWarmPoolAcrossReload(t *testing.T) {
	server, gateway := newGuardedServer(t)
	value := warmPool(t, server, gateway)
	probes := gateway.calls.Load()

	for round := 0; round < 3; round++ {
		applyConfig(t, server, guardJSON(gateway.base()))
	}

	got, ok := server.state().tickets.Value(testAccountID, testModel)
	if !ok || got != value {
		t.Fatalf("重建后取值 = %q/%v，期望沿用旧票 %q", got, ok, value)
	}
	if calls := gateway.calls.Load(); calls != probes {
		t.Fatalf("重建期间又撞了 %d 次票，期望 0 次", calls-probes)
	}
	// 另一个模型不能借用这张票：票按 (账号 × 模型) 分池，绝不跨模型复用。
	if value, ok := server.state().tickets.Value(testAccountID, "gpt-5"); ok {
		t.Fatalf("模型 gpt-5 不应有票，却拿到 %q", value)
	}
}

func TestApplyConfigSwitchesAccountsAndWipesCredential(t *testing.T) {
	server, gateway := newGuardedServer(t)
	warmPool(t, server, gateway)

	// 换成另一个账号：旧账号立即不再被接管。
	raw := fmt.Appendf(nil, `{"overload_guard":{"enabled":true,
		"accounts":[{"account_id":77,"enabled":true}],
		"ticket_pool":{"models":[%q],"follow_observed_models":false,"gateway_base_url":%q},
		"proxy_pool":{"enabled":false}}}`, testModel, gateway.base())
	applyConfig(t, server, raw)
	if server.state().tickets.Enabled(testAccountID) {
		t.Fatal("被移除的账号不应仍被接管")
	}
	if !server.state().tickets.Enabled(77) {
		t.Fatal("新账号应当被接管")
	}

	// 再换回来：管理员一取消接管，那个账号的 OAuth 令牌与票就该从内存里消失，
	// 不能因为后来又被加回来而复活。
	applyConfig(t, server, guardJSON(gateway.base()))
	status := server.state().tickets.Status()
	if len(status.Accounts) != 1 || status.Accounts[0].AccountID != testAccountID {
		t.Fatalf("账号列表 = %+v", status.Accounts)
	}
	if status.Accounts[0].HasCredential {
		t.Fatal("取消接管过的账号，其凭据必须已经被抹掉")
	}
	if status.Valid != 0 {
		t.Fatalf("取消接管过的账号还剩 %d 张票", status.Valid)
	}
}

func TestTestConfigReportsLiveTicketStatus(t *testing.T) {
	server, gateway := newGuardedServer(t)
	value := warmPool(t, server, gateway)

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if !result.GetSuccess() {
		t.Fatalf("票池正常时应当成功: %s", result.GetMessage())
	}
	if result.GetLatencyMs() < 0 {
		t.Fatalf("延迟 = %d", result.GetLatencyMs())
	}

	human, board := splitDashboard(t, result.GetMessage())
	if !strings.Contains(human, "票池：1/1 张有效") {
		t.Fatalf("应当报出整体水位: %q", human)
	}
	if !strings.Contains(human, fmt.Sprintf("账号 %d（主力号）", testAccountID)) {
		t.Fatalf("应当报出账号明细: %q", human)
	}
	if !strings.Contains(human, testModel+" 1/1") {
		t.Fatalf("应当按模型报出票数: %q", human)
	}
	if !strings.Contains(human, "代理库：已关闭") {
		t.Fatalf("应当说明代理库状态: %q", human)
	}

	if board.Synced == nil || !*board.Synced {
		t.Fatal("传入的就是当前生效配置，synced 应为真")
	}
	if board.Tickets.Valid != 1 || board.Tickets.Wanted != 1 {
		t.Fatalf("看板票数 = %d/%d", board.Tickets.Valid, board.Tickets.Wanted)
	}
	if board.Guard.TargetLength != pluginconfig.DefaultTargetStateLength {
		t.Fatalf("看板默认满血长度 = %d", board.Guard.TargetLength)
	}
	if board.Guard.ModelLengthOverrides != len(pluginconfig.DefaultPoolModels()) {
		t.Fatalf("看板单独配置长度的模型数 = %d", board.Guard.ModelLengthOverrides)
	}
	if len(board.Tickets.Accounts) != 1 {
		t.Fatalf("看板账号数 = %d", len(board.Tickets.Accounts))
	}
	account := board.Tickets.Accounts[0]
	if !account.HasCredential || account.CredentialSeconds < 0 {
		t.Fatalf("看板凭据状态 = %+v", account)
	}
	if len(account.Models) != 1 || account.Models[0].Model != testModel || account.Models[0].Valid != 1 {
		t.Fatalf("看板模型明细 = %+v", account.Models)
	}
	// 每张卡片带自己的长度口径：gpt-5-codex 不在默认覆盖表里，落到兜底值。
	if account.Models[0].TargetLength != pluginconfig.DefaultTargetStateLength {
		t.Fatalf("看板卡片口径 = %d", account.Models[0].TargetLength)
	}
	if account.Models[0].FreshestSeconds <= 0 {
		t.Fatalf("看板应当报出剩余有效期: %+v", account.Models[0])
	}

	// 看板会经宿主接口回到浏览器：票与令牌一个字节都不能跟着出去。
	message := result.GetMessage()
	if strings.Contains(message, value) || strings.Contains(message, testBearer) {
		t.Fatalf("看板泄露了票或令牌: %q", message)
	}
}

// 代理这行是管理员判断「填进去的出口到底有没有在用」的唯一入口：
// 条数、协议构成、累计成败都要在，地址则一律打码。
func TestFormatProxyLineAndDashboardEntries(t *testing.T) {
	config := pluginconfig.Proxy{Enabled: true, Proxies: []pluginconfig.ProxyItem{
		{Name: "hk", URL: "socks5://1.2.3.4:1080", Enabled: true},
		{Name: "hk2", URL: "socks5://1.2.3.5:1080", Enabled: true},
		{Name: "us", URL: "http://u:s3cr3t@10.0.0.9:8080", Enabled: true},
	}}
	pool := proxypool.New(config, time.Now)
	pool.Note("socks5://1.2.3.4:1080", true)
	pool.Note("socks5://1.2.3.4:1080", true)
	pool.Note("http://u:s3cr3t@10.0.0.9:8080", false)
	status := pool.Status()

	line := formatProxyLine(config, status, true)
	want := "代理库：3 条（socks5 2 / http 1），撞票成功 2 次、失败 1 次。"
	if line != want {
		t.Fatalf("代理行 = %q，期望 %q", line, want)
	}

	// 关闭时的两种说法要分得清：只是关了代理，还是连出口都没了。
	off := config
	off.Enabled = false
	if got := formatProxyLine(off, proxypool.New(off, time.Now).Status(), true); !strings.Contains(got, "只走直连") {
		t.Fatalf("关闭且允许直连时 = %q", got)
	}
	if got := formatProxyLine(off, proxypool.New(off, time.Now).Status(), false); !strings.Contains(got, "没有任何撞票出口") {
		t.Fatalf("关闭且禁直连时 = %q", got)
	}

	// 看板条目经宿主接口回到浏览器，地址与凭据都不能原样出去。
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal 出错: %v", err)
	}
	for _, leaked := range []string{"s3cr3t", "1.2.3.4", "10.0.0.9"} {
		if strings.Contains(string(encoded), leaked) {
			t.Fatalf("看板泄露了 %q: %s", leaked, encoded)
		}
	}
}

// 配置页点一次「刷新状态」就调一次 TestConfig，所以它必须是纯只读的：
// 一旦它会发出撞票或探测流量，一个刷新按钮就变成了流量放大器。
func TestTestConfigMakesNoNetworkCalls(t *testing.T) {
	server, gateway := newGuardedServer(t)
	warmPool(t, server, gateway)
	before := gateway.calls.Load()

	for round := 0; round < 5; round++ {
		if _, err := server.TestConfig(context.Background(),
			&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())}); err != nil {
			t.Fatalf("TestConfig 出错: %v", err)
		}
	}
	if after := gateway.calls.Load(); after != before {
		t.Fatalf("TestConfig 发出了 %d 次出站请求，期望 0 次", after-before)
	}
}

// 看板的自动刷新走 plugin.status → Health.status_json：宿主不会为它弹提示，
// 所以配置页可以每几秒读一次。这条通道必须给出与 config.test 同样的快照，
// 而且同样只读内存、不发任何请求。
func TestHealthCarriesDashboardStatusJSON(t *testing.T) {
	server, gateway := newGuardedServer(t)
	value := warmPool(t, server, gateway)
	before := gateway.calls.Load()

	var board dashboard
	for round := 0; round < 5; round++ {
		health, err := server.Health(context.Background(), &pluginv1.HealthRequest{})
		if err != nil {
			t.Fatalf("Health 出错: %v", err)
		}
		if err := json.Unmarshal([]byte(health.GetStatusJson()), &board); err != nil {
			t.Fatalf("status_json 不是合法看板 JSON: %v（%q）", err, health.GetStatusJson())
		}
		// 轮询不能把票或令牌带出去，也不能顺手发出站请求。
		if strings.Contains(health.GetStatusJson(), value) || strings.Contains(health.GetStatusJson(), testBearer) {
			t.Fatalf("status_json 泄露了票或令牌: %q", health.GetStatusJson())
		}
	}
	if after := gateway.calls.Load(); after != before {
		t.Fatalf("Health 发出了 %d 次出站请求，期望 0 次", after-before)
	}

	if board.Tickets.Valid != 1 || board.Tickets.Wanted != 1 {
		t.Fatalf("看板票数 = %d/%d", board.Tickets.Valid, board.Tickets.Wanted)
	}
	if len(board.Tickets.Accounts) != 1 || board.Tickets.Accounts[0].AccountID != testAccountID {
		t.Fatalf("看板账号 = %+v", board.Tickets.Accounts)
	}
	if !board.Guard.Enabled || board.Guard.TargetLength != pluginconfig.DefaultTargetStateLength {
		t.Fatalf("看板总览 = %+v", board.Guard)
	}
	// Health 拿不到宿主存的那份配置，无从比对，所以 synced 必须缺省而不是谎报为真。
	if board.Synced != nil {
		t.Fatalf("Health 不该给出 synced: %v", *board.Synced)
	}
}

// 老宿主（< 0.2.7）的 TestConfigResponse 没有 status_json 字段，只会把 message
// 原样转发，所以哨兵行必须继续存在，且两处内容完全一致。
func TestTestConfigStatusJSONMatchesSentinelLine(t *testing.T) {
	server, gateway := newGuardedServer(t)
	warmPool(t, server, gateway)

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	message := result.GetMessage()
	index := strings.Index(message, StatusSentinel)
	if index < 0 {
		t.Fatalf("消息里没有哨兵行: %q", message)
	}
	if sentinel := message[index+len(StatusSentinel):]; sentinel != result.GetStatusJson() {
		t.Fatalf("哨兵行与 status_json 不一致:\n哨兵 %q\nJSON %q", sentinel, result.GetStatusJson())
	}
}

// 管理员在界面上改了配置还没保存就点测试时，看板展示的是「当前生效」的状态，
// 必须明说这一点，否则改动看起来像是没生效。
func TestTestConfigFlagsUnappliedConfig(t *testing.T) {
	server, gateway := newGuardedServer(t)
	warmPool(t, server, gateway)

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: []byte(`{}`)})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	human, board := splitDashboard(t, result.GetMessage())
	if board.Synced == nil || *board.Synced {
		t.Fatal("传入的不是当前生效配置，synced 应为假")
	}
	if !strings.Contains(human, "还没有被应用") {
		t.Fatalf("应当提示配置尚未应用: %q", human)
	}
}

// 撞到的 state 长度不等于该模型的满血长度时，池会一直是空的。
// 这不是「还在预热」，必须报成失败并给出可操作的排查方向。
func TestTestConfigReportsNoHealthyTicket(t *testing.T) {
	server, gateway := newGuardedServer(t)
	gateway.length.Store(312)
	harvest(server, gateway)
	if added := server.state().tickets.Refill(context.Background(), testAccountID, testModel); added != 0 {
		t.Fatalf("长度不符的票不应入池，却入了 %d 张", added)
	}
	// 撞不到满血票会换出口重试 retry_rounds 轮，所以一次 Refill 会发 1+3 次探针。
	if probes := gateway.calls.Load(); probes != 1+pluginconfig.DefaultRetryRounds {
		t.Fatalf("探针发了 %d 次，期望 %d 次", probes, 1+pluginconfig.DefaultRetryRounds)
	}

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if result.GetSuccess() {
		t.Fatalf("撞过一轮却一张票都没有时应当失败: %s", result.GetMessage())
	}
	human, board := splitDashboard(t, result.GetMessage())
	if !strings.Contains(human, "满血票长度") {
		t.Fatalf("应当提示检查满血票长度: %q", human)
	}
	if board.Tickets.Valid != 0 {
		t.Fatalf("看板票数 = %d", board.Tickets.Valid)
	}
	models := board.Tickets.Accounts[0].Models
	if len(models) != 1 || models[0].Grades["mismatch"] != 1+pluginconfig.DefaultRetryRounds {
		t.Fatalf("看板应当把这几轮都记成 mismatch: %+v", models)
	}
	if models[0].Detail == "" || !strings.Contains(models[0].Detail, "mismatch") {
		t.Fatalf("看板应当带上各出口的分级摘要: %q", models[0].Detail)
	}
}

// 账号在上游被降级时，撞回来的 state 长度可能正好对得上，只有上游自报的模型名能拆穿。
// 这种情况下「调长度」是错的建议：排错文案必须指向账号的模型权限，而不是长度口径。
func TestTestConfigReportsUpstreamDowngrade(t *testing.T) {
	server, gateway := newGuardedServer(t)
	gateway.served.Store("gpt-5.6-luna")
	harvest(server, gateway)
	if added := server.state().tickets.Refill(context.Background(), testAccountID, testModel); added != 0 {
		t.Fatalf("上游换了模型服务，这些票不属于本模型，却入了 %d 张", added)
	}

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if result.GetSuccess() {
		t.Fatalf("撞了一轮全是降级票时应当失败: %s", result.GetMessage())
	}
	human, board := splitDashboard(t, result.GetMessage())
	if strings.Contains(human, "调整为上游当前实际返回的长度") {
		t.Fatalf("全是降级票时不该建议调长度: %q", human)
	}
	if !strings.Contains(human, "用别的模型服务") {
		t.Fatalf("应当指出上游换了模型服务: %q", human)
	}
	models := board.Tickets.Accounts[0].Models
	if len(models) != 1 || models[0].Grades["downgraded"] != 1+pluginconfig.DefaultRetryRounds {
		t.Fatalf("看板应当把这几轮都记成 downgraded: %+v", models)
	}
	if !strings.Contains(models[0].Detail, "gpt-5.6-luna") {
		t.Fatalf("看板应当点明是谁替它服务的: %q", models[0].Detail)
	}
}

// 还没有任何账号有凭据时，池空是正常的预热中状态，不能报成失败——
// 否则管理员刚装好插件就会看到一片红。
func TestTestConfigTreatsWarmupAsHealthy(t *testing.T) {
	server, gateway := newGuardedServer(t)

	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: guardJSON(gateway.base())})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if !result.GetSuccess() {
		t.Fatalf("预热中不应报失败: %s", result.GetMessage())
	}
	human, _ := splitDashboard(t, result.GetMessage())
	if !strings.Contains(human, "还没有凭据") {
		t.Fatalf("应当说明账号在等第一个真实请求: %q", human)
	}
}

func TestTestConfigWhenGuardDisabled(t *testing.T) {
	server := newServer(t)
	raw := []byte(`{"overload_guard":{"enabled":false}}`)
	applyConfig(t, server, raw)

	result, err := server.TestConfig(context.Background(), &pluginv1.TestConfigRequest{ConfigJson: raw})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if !result.GetSuccess() {
		t.Fatalf("总开关关闭是合法状态: %s", result.GetMessage())
	}
	human, board := splitDashboard(t, result.GetMessage())
	if !strings.Contains(human, "总开关已关闭") {
		t.Fatalf("应当说明原因: %q", human)
	}
	if board.Guard.Enabled {
		t.Fatal("看板应当反映总开关已关闭")
	}
}

func TestTestConfigRejectsInvalidConfig(t *testing.T) {
	server := newServer(t)
	result, err := server.TestConfig(context.Background(),
		&pluginv1.TestConfigRequest{ConfigJson: []byte(`{"overload_guard":{"override_mode":"maybe"}}`)})
	if err != nil {
		t.Fatalf("TestConfig 出错: %v", err)
	}
	if result.GetSuccess() {
		t.Fatal("非法配置的测试应当失败")
	}
	if strings.Contains(result.GetMessage(), StatusSentinel) {
		t.Fatal("配置都没解析成功，不该回看板数据")
	}
}

// 撞票走的是独立连接池：proxy_mode 说的是「上游请求要不要沿用宿主下发的代理」，
// 与撞票出口无关，所以管理员把它关成 disabled 时，撞票仍然要能用 socks5。
func TestCapturePoolConfigForcesHostProxyMode(t *testing.T) {
	config, err := pluginconfig.Parse([]byte(`{"proxy_mode":"disabled","extra_headers":{"X-Trace":"plugin"}}`))
	if err != nil {
		t.Fatalf("解析配置失败: %v", err)
	}
	forCapture := capturePoolConfig(config)
	if forCapture.ProxyMode != pluginconfig.ProxyModeHost {
		t.Fatalf("撞票连接池的 proxy_mode = %q，期望 host", forCapture.ProxyMode)
	}
	if len(forCapture.ExtraHeaders) != 0 {
		t.Fatal("上游用的 extra_headers 不应带到撞票探针上")
	}
	if config.ProxyMode != pluginconfig.ProxyModeDisabled || len(config.ExtraHeaders) != 1 {
		t.Fatal("原配置不应被修改")
	}
}
