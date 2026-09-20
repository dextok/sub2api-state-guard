package ticketpool

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

const managedAccount = int64(42)

// fakeProxies 冒充 proxypool.Pool：按轮次吐出预设的出口（完整代理 URL），并记录反馈。
type fakeProxies struct {
	mu sync.Mutex
	// addrs 是库里的全部出口（socks5://… / http://…）；Lease 按顺序取未被 skip 的那些。
	addrs []string
	// notes 按地址记录最后一次反馈。
	notes map[string]bool
	// leases 记录每次 Lease 的返回值，用于断言「换代理重试」。
	leases [][]string
}

func newFakeProxies(addrs ...string) *fakeProxies {
	return &fakeProxies{addrs: addrs, notes: make(map[string]bool)}
}

func (p *fakeProxies) Lease(count int, skip map[string]struct{}) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, count)
	for _, addr := range p.addrs {
		if len(out) >= count {
			break
		}
		if _, skipped := skip[addr]; skipped {
			continue
		}
		out = append(out, addr)
	}
	p.leases = append(p.leases, out)
	return out
}

func (p *fakeProxies) Note(addr string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notes[addr] = ok
}

func (p *fakeProxies) snapshot() ([][]string, map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	notes := make(map[string]bool, len(p.notes))
	for addr, ok := range p.notes {
		notes[addr] = ok
	}
	return append([][]string(nil), p.leases...), notes
}

// managerHarness 把 Manager 的全部外部依赖都换成本地可控的桩。
type managerHarness struct {
	manager *Manager
	stub    *captureStub
	proxies *fakeProxies
	now     time.Time
}

func newManagerHarness(t *testing.T, tune func(*pluginconfig.Ticket), proxies ProxyProvider) *managerHarness {
	t.Helper()
	stub := newCaptureStub(t)
	stub.reply(http.StatusOK, state(292), sseText)

	ticket := pluginconfig.DefaultTicketPool()
	ticket.Models = []string{"gpt-5-codex"}
	ticket.FollowObservedModels = false
	ticket.PoolSize = 1
	ticket.RetryRounds = 0
	ticket.IncludeDirect = true
	ticket.ProxiesPerRound = 0
	ticket.CheckIntervalSeconds = 3600
	ticket.ProbeTimeoutSeconds = 5
	ticket.GatewayBaseURL = stub.server.URL + "/backend-api/codex"
	if tune != nil {
		tune(&ticket)
	}

	harness := &managerHarness{stub: stub, proxies: nil, now: time.Unix(1700000000, 0)}
	if concrete, ok := proxies.(*fakeProxies); ok {
		harness.proxies = concrete
	}

	config := pluginconfig.Default()
	config.OverloadGuard.Enabled = true
	config.OverloadGuard.TicketPool = ticket
	config.OverloadGuard.Accounts = []pluginconfig.Account{
		{AccountID: managedAccount, Enabled: true, Note: "主力号"},
	}
	harness.manager = New(Options{
		Config:  config,
		Clients: directClients{client: stub.server.Client()},
		Proxies: proxies,
		Now:     func() time.Time { return harness.now },
		// 首轮撞票的抖动在测试里没有意义，只会让每个用例多等几秒。
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	return harness
}

// harvest 模拟一个真实请求经过插件，把凭据采进内存。
func (h *managerHarness) harvest(accountID int64) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+repeatTo(40))
	header.Set("ChatGpt-Account-Id", "acct-9")
	h.manager.Harvest(accountID, header, h.stub.server.URL+"/backend-api/codex/responses")
}

// 一个账号必须先有一个真实请求经过，它的票池才会开始预热。
func TestHarvestOnlyForManagedAccounts(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)

	harness.harvest(managedAccount)
	if !harness.manager.credentials.Has(managedAccount) {
		t.Fatal("被接管的账号应当采到凭据")
	}

	harness.harvest(999)
	if harness.manager.credentials.Has(999) {
		t.Fatal("没被接管的账号，它的令牌不该进插件内存")
	}
}

// 没有凭据就撞不了票，看板要明说原因而不是显示成「正在维护」。
func TestRefillWithoutCredential(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)

	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 0 {
		t.Fatalf("补入 %d 张", added)
	}
	status := harness.manager.Status()
	if len(status.Accounts) != 1 || status.Accounts[0].HasCredential {
		t.Fatalf("状态 = %+v", status.Accounts)
	}
	model := status.Accounts[0].Models[0]
	if model.Reason != "no-credential" || model.Detail == "" {
		t.Fatalf("模型状态 = %+v", model)
	}
}

func TestRefillFillsPoolAndServesValue(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)
	harness.harvest(managedAccount)

	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 1 {
		t.Fatalf("补入 %d 张，期望 1 张", added)
	}
	if !harness.manager.Ready(managedAccount) {
		t.Fatal("有票之后应当 Ready")
	}
	value, ok := harness.manager.Value(managedAccount, "gpt-5-codex")
	if !ok || len(value) != 292 {
		t.Fatalf("取到的票长度 = %d", len(value))
	}
	if _, ok := harness.manager.Value(managedAccount, "gpt-5"); ok {
		t.Fatal("票按模型铸造，不能跨模型复用")
	}
	if _, ok := harness.manager.Value(999, "gpt-5-codex"); ok {
		t.Fatal("没被接管的账号不该拿到票")
	}

	// 池已满且票还新，下一轮应当什么都不做。
	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 0 {
		t.Fatalf("池满时又补了 %d 张", added)
	}
	if reason := harness.manager.Status().Accounts[0].Models[0].Reason; reason != "full" {
		t.Fatalf("判定 = %q，期望 full", reason)
	}
}

// 满血长度按模型不同，所以同一个账号下两个池各按各的口径收票：
// 上游回 312 时，口径 312 的池收得到，落到兜底 292 的池一张也收不到。
func TestRefillUsesPerModelTargetLength(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.Models = []string{"gpt-5-codex", "gpt-6-astra"}
		ticket.TargetStateLength = 292
		ticket.ModelStateLengths = map[string]int{"gpt-6-astra": 312}
	}, nil)
	harness.harvest(managedAccount)
	harness.stub.reply(http.StatusOK, state(312), sseText)

	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-6-astra"); added != 1 {
		t.Fatalf("astra 的口径是 312，应当补入 1 张，实际 %d 张", added)
	}
	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 0 {
		t.Fatalf("gpt-5-codex 落到兜底的 292，312 的票不该入池，实际补入 %d 张", added)
	}
	if value, ok := harness.manager.Value(managedAccount, "gpt-6-astra"); !ok || len(value) != 312 {
		t.Fatalf("astra 应当取到一张 312 的票，ok=%v 长度=%d", ok, len(value))
	}
	if _, ok := harness.manager.Value(managedAccount, "gpt-5-codex"); ok {
		t.Fatal("gpt-5-codex 池是空的，不该取到票")
	}

	// 每张卡片都要带自己的口径，否则看板上「满血」二字没法核对。
	want := map[string]int{"gpt-5-codex": 292, "gpt-6-astra": 312}
	models := harness.manager.Status().Accounts[0].Models
	if len(models) != len(want) {
		t.Fatalf("卡片数 = %d，期望 %d", len(models), len(want))
	}
	for _, model := range models {
		if model.TargetLength != want[model.Model] {
			t.Fatalf("%s 卡片的口径 = %d，期望 %d", model.Model, model.TargetLength, want[model.Model])
		}
	}
}

// 撞不到满血票时换未用过的出口重试 retry_rounds 轮，而不是死等下一个周期。
func TestRefillRetriesWithFreshProxies(t *testing.T) {
	proxies := newFakeProxies("socks5://1.1.1.1:1080", "socks5://2.2.2.2:1080", "http://3.3.3.3:8080")
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.IncludeDirect = false
		ticket.ProxiesPerRound = 1
		ticket.RetryRounds = 2
	}, proxies)
	harness.harvest(managedAccount)
	// 全是 312：有产出但长度不符，一张也入不了池。
	harness.stub.reply(http.StatusOK, state(312), sseText)

	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 0 {
		t.Fatalf("补入 %d 张", added)
	}

	leases, notes := proxies.snapshot()
	if len(leases) != 3 {
		t.Fatalf("取了 %d 轮代理，期望 1+2 轮", len(leases))
	}
	used := map[string]bool{}
	for _, lease := range leases {
		if len(lease) != 1 {
			t.Fatalf("每轮应当只取 1 个出口，实际 %v", lease)
		}
		if used[lease[0]] {
			t.Fatalf("重试时又用了同一个出口: %v", leases)
		}
		used[lease[0]] = true
	}
	// 正常收到响应说明出口是通的，不该被记成失败。
	for addr, ok := range notes {
		if !ok {
			t.Fatalf("出口 %s 被误判为失败", addr)
		}
	}

	status := harness.manager.Status().Accounts[0].Models[0]
	if status.Grades["mismatch"] != 3 {
		t.Fatalf("分级 = %v，期望三轮各一次 mismatch", status.Grades)
	}
	if status.Detail == "" {
		t.Fatal("一轮没补到票时看板要给出各出口的分级摘要")
	}
}

// 连 state 都没拿到说明不是出口的问题，换代理也没意义，应当立刻收手。
func TestRefillStopsRetryingWhenNoStateAtAll(t *testing.T) {
	proxies := newFakeProxies("socks5://1.1.1.1:1080", "socks5://2.2.2.2:1080", "http://3.3.3.3:8080")
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.IncludeDirect = false
		ticket.ProxiesPerRound = 1
		ticket.RetryRounds = 2
	}, proxies)
	harness.harvest(managedAccount)
	harness.stub.reply(http.StatusForbidden, "", "forbidden")

	harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex")

	leases, _ := proxies.snapshot()
	if len(leases) != 2 {
		t.Fatalf("取了 %d 轮代理，期望第 2 轮就收手", len(leases))
	}
}

// 连响应都没拿到才算出口失败：撞票被网关拒了是账号的事，不该记到出口头上，
// 否则看板上每条代理的成败计数就失去了参考价值。
func TestRoundNotesProxyFailureOnlyOnTransportError(t *testing.T) {
	proxies := newFakeProxies("socks5://1.1.1.1:1080")
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.IncludeDirect = false
		ticket.ProxiesPerRound = 1
		ticket.RetryRounds = 0
	}, proxies)
	harness.harvest(managedAccount)
	harness.stub.server.Close()

	harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex")

	_, notes := proxies.snapshot()
	if ok, exists := notes["socks5://1.1.1.1:1080"]; !exists || ok {
		t.Fatalf("出口反馈 = %v，期望记为失败", notes)
	}
}

func TestRoundWithoutAnyOutlet(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.IncludeDirect = false
		ticket.ProxiesPerRound = 2
	}, nil)
	harness.harvest(managedAccount)

	if added := harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex"); added != 0 {
		t.Fatalf("补入 %d 张", added)
	}
	detail := harness.manager.Status().Accounts[0].Models[0].Detail
	if !strings.Contains(detail, "没有可用的撞票出口") {
		t.Fatalf("看板摘要 = %q", detail)
	}
}

// 配置里没写全模型时的唯一兜底：请求里出现的新模型也要获得自己的池。
func TestObserveTracksNewModels(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.FollowObservedModels = true
	}, nil)

	harness.manager.Observe(managedAccount, "gpt-5-pro")
	harness.manager.Observe(managedAccount, "gpt-5-codex") // 配置里已有，不重复跟踪
	harness.manager.Observe(999, "gpt-6")                  // 没被接管
	harness.manager.Observe(managedAccount, "")            // 解析不出模型名
	harness.manager.Observe(managedAccount, repeatTo(pluginconfig.MaxModelNameBytes+1))
	harness.manager.Observe(managedAccount, "bad\nmodel")

	models := harness.manager.modelsFor(managedAccount)
	if len(models) != 2 || models[0] != "gpt-5-codex" || models[1] != "gpt-5-pro" {
		t.Fatalf("要维护的模型 = %v", models)
	}
	if len(harness.manager.modelsFor(999)) != 1 {
		t.Fatal("没被接管的账号只应有配置里的模型")
	}
}

func TestObserveIsOffWhenFollowDisabled(t *testing.T) {
	harness := newManagerHarness(t, nil, nil) // FollowObservedModels=false

	harness.manager.Observe(managedAccount, "gpt-5-pro")

	models := harness.manager.modelsFor(managedAccount)
	if len(models) != 1 || models[0] != "gpt-5-codex" {
		t.Fatalf("要维护的模型 = %v，期望只有配置里写的", models)
	}
}

// 模型久不出现就不再维护它的池，免得池键被历史请求撑满。
func TestObservedModelsExpireAndEvict(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.FollowObservedModels = true
	}, nil)

	harness.manager.Observe(managedAccount, "stale")
	harness.now = harness.now.Add(observedModelTTL + time.Minute)
	harness.manager.Observe(managedAccount, "fresh")

	models := harness.manager.modelsFor(managedAccount)
	if len(models) != 2 || models[1] != "gpt-5-codex" || models[0] != "fresh" {
		t.Fatalf("要维护的模型 = %v，期望过期的 stale 被清掉", models)
	}

	// 跟踪数有上限：超了就淘汰最久没出现的那个。
	for index := 0; index < maxObservedModels+4; index++ {
		harness.now = harness.now.Add(time.Second)
		harness.manager.Observe(managedAccount, "m"+string(rune('a'+index)))
	}
	if tracked := len(harness.manager.observed[managedAccount]); tracked != maxObservedModels {
		t.Fatalf("跟踪了 %d 个模型，期望被 maxObservedModels 限制在 %d 个", tracked, maxObservedModels)
	}
}

// 自动跟踪的模型过期后，它的池要在票用完之后从内存与看板里消失，而不是永久留一张卡片。
func TestObservedModelPoolIsPrunedOnceEmpty(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.FollowObservedModels = true
		// 票的有效期要比自动跟踪的 TTL 长，才能分开观察「模型过期但票还在」与「票也没了」两步。
		ticket.TicketTTLSeconds = 86400
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		harness.manager.loopWG.Wait()
	}()

	harness.harvest(managedAccount)
	harness.manager.Observe(managedAccount, "gpt-5-pro")
	harness.manager.reconcile(ctx)
	waitFor(t, "自动跟踪模型的首轮撞票完成", func() bool {
		for _, model := range harness.manager.store.ModelStatuses(managedAccount, 1, harness.now) {
			if model.Model == "gpt-5-pro" && model.Valid == 1 && model.LastRefillSeconds >= 0 {
				return true
			}
		}
		return false
	})

	// 模型过期但票还有效：循环停掉，池保留到票过期。
	harness.now = harness.now.Add(observedModelTTL + time.Minute)
	harness.manager.reconcile(ctx)
	if loops := harness.manager.loopCount(); loops != 1 {
		t.Fatalf("过期模型的循环没停，还剩 %d 个", loops)
	}
	if models := harness.manager.store.Models(managedAccount); len(models) != 2 {
		t.Fatalf("票还有效时池不该被收走：%v", models)
	}

	// 票也过期了：下一次对账把空池收走。
	harness.now = harness.now.Add(24*time.Hour + time.Minute)
	harness.manager.reconcile(ctx)
	if models := harness.manager.store.Models(managedAccount); len(models) != 1 || models[0] != "gpt-5-codex" {
		t.Fatalf("过期模型的空池应被收走，实际 %v", models)
	}
}

// 配置里写了但还没建池的模型也要出现在看板上，否则管理员会以为自己配的被忽略了。
func TestStatusShowsConfiguredModelsBeforeAnyTicket(t *testing.T) {
	harness := newManagerHarness(t, func(ticket *pluginconfig.Ticket) {
		ticket.Models = []string{"gpt-5-codex", "gpt-5-pro"}
		ticket.PoolSize = 3
	}, nil)

	status := harness.manager.Status()
	if status.Running {
		t.Fatal("还没 Start 不该显示为运行中")
	}
	if len(status.Accounts) != 1 {
		t.Fatalf("账号数 = %d", len(status.Accounts))
	}
	account := status.Accounts[0]
	if account.Note != "主力号" || account.HasCredential || account.CredentialSeconds != -1 {
		t.Fatalf("账号状态 = %+v", account)
	}
	if len(account.Models) != 2 || account.Models[0].Model != "gpt-5-codex" ||
		account.Models[1].Model != "gpt-5-pro" {
		t.Fatalf("模型列表 = %+v", account.Models)
	}
	if status.Valid != 0 || status.Wanted != 6 {
		t.Fatalf("水位 = %d/%d，期望 0/6", status.Valid, status.Wanted)
	}
}

// 看板经宿主接口回到浏览器，只允许出现计数与时间，绝不能带上 state 或令牌本身。
func TestStatusNeverExposesCredentialOrTicket(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)
	harness.harvest(managedAccount)
	harness.manager.Refill(context.Background(), managedAccount, "gpt-5-codex")
	harness.now = harness.now.Add(90 * time.Second)

	status := harness.manager.Status()
	account := status.Accounts[0]
	if !account.HasCredential || account.CredentialSeconds != 90 {
		t.Fatalf("凭据状态 = %+v", account)
	}
	if status.Valid != 1 || status.Wanted != 1 {
		t.Fatalf("水位 = %d/%d", status.Valid, status.Wanted)
	}

	encoded := mustJSON(t, status)
	if containsAny(encoded, repeatTo(40), "Bearer", state(292)) {
		t.Fatalf("看板输出泄露了凭据或票: %s", encoded)
	}
}

// 账号刚被采到凭据就该把池拉起来；没有凭据的账号不开循环，也不占看板位置。
func TestReconcileStartsPoolsOnlyAfterCredential(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		harness.manager.loopWG.Wait()
	}()

	harness.manager.reconcile(ctx)
	if loops := harness.manager.loopCount(); loops != 0 {
		t.Fatalf("没有凭据时起了 %d 个补池循环", loops)
	}

	harness.harvest(managedAccount)
	harness.manager.reconcile(ctx)
	if loops := harness.manager.loopCount(); loops != 1 {
		t.Fatalf("起了 %d 个补池循环，期望 1 个", loops)
	}
	// 重复对账不该把同一个池开两遍。
	harness.manager.reconcile(ctx)
	if loops := harness.manager.loopCount(); loops != 1 {
		t.Fatalf("重复对账后有 %d 个循环", loops)
	}

	waitFor(t, "首轮撞票完成", func() bool {
		return harness.manager.store.Valid(managedAccount, "gpt-5-codex", harness.now) == 1
	})
}

// 管理员一取消接管，对应的补池循环要停掉，凭据与票也要被抹掉。
func TestReconcileStopsPoolsWhenCredentialGoesAway(t *testing.T) {
	harness := newManagerHarness(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		harness.manager.loopWG.Wait()
	}()

	harness.harvest(managedAccount)
	harness.manager.reconcile(ctx)
	waitFor(t, "首轮撞票完成", func() bool {
		return harness.manager.store.Valid(managedAccount, "gpt-5-codex", harness.now) == 1
	})

	harness.manager.credentials.Retain(nil)
	harness.manager.reconcile(ctx)
	if loops := harness.manager.loopCount(); loops != 0 {
		t.Fatalf("凭据没了还剩 %d 个补池循环", loops)
	}
}

func TestFormatGradesIsStable(t *testing.T) {
	got := formatGrades(map[Grade]int{GradeMismatch: 3, GradeHealthy: 1, GradeError: 2})
	if got != "error=2 healthy=1 mismatch=3" {
		t.Fatalf("格式化结果 = %q", got)
	}
}

// 嗅探到的模型名按与配置项 models 相同的字符集与长度规则决定能不能建池。
func TestTrackableModel(t *testing.T) {
	cases := map[string]bool{
		"gpt-5-codex":                            true,
		"org/gpt-5.1:latest":                     true,
		"":                                       false,
		"bad\nmodel":                             false,
		"gpt 5":                                  false,
		"gpt-5-中文":                               false,
		repeatTo(pluginconfig.MaxModelNameBytes): true,
		repeatTo(pluginconfig.MaxModelNameBytes + 1): false,
	}
	for model, want := range cases {
		if got := trackableModel(model); got != want {
			t.Errorf("trackableModel(%q, %d 字节) = %v，期望 %v", model, len(model), got, want)
		}
	}
}

func (m *Manager) loopCount() int {
	m.loopsMu.Lock()
	defer m.loopsMu.Unlock()
	return len(m.loops)
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", what)
}

func containsAny(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if needle != "" && strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	return string(raw)
}
