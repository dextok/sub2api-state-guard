package ticketpool

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

const (
	// superviseInterval 是「哪些 (账号 × 模型) 该有池」的对账间隔。
	// 账号刚被采到凭据、请求里出现新模型，都靠这一轮对账把池拉起来。
	superviseInterval = 5 * time.Second
	// maxConcurrentProbes 限制全局在途探针数：账号数 × 模型数 × 每轮代理数
	// 很容易上百，不设闸会把出站连接打满。
	maxConcurrentProbes = 16
	// maxObservedModels 是每个账号自动跟踪的模型数上限。
	maxObservedModels = 16
	// observedModelTTL 是自动跟踪的模型多久没再出现就不再维护它的池。
	observedModelTTL = time.Hour
	// startupJitter 让各个池的首轮撞票错开，避免进程启动瞬间齐发。
	startupJitter = 5 * time.Second
)

// ProxyProvider 提供撞票要用的 http/socks5 出口，由 proxypool.Pool 实现。
type ProxyProvider interface {
	// Lease 随机取最多 count 条代理地址（完整 URL，如 socks5://1.2.3.4:1080），
	// skip 里的不再返回。
	Lease(count int, skip map[string]struct{}) []string
	// Note 反馈一次使用结果，只累计计数，不会因失败剔除代理。
	Note(proxyURL string, ok bool)
}

// Options 是构造 Manager 所需的依赖。
type Options struct {
	Config   pluginconfig.Config
	Registry *Registry
	Clients  ClientProvider
	Proxies  ProxyProvider
	Now      func() time.Time
	Log      func(string, ...any)
	// Jitter 可注入以便测试；缺省为 [0, d) 上的均匀分布。
	Jitter func(time.Duration) time.Duration
}

// Manager 负责维护所有 (账号 × 模型) 票池，并作为 transport.ValueSource 向转发路径供票。
type Manager struct {
	guard        pluginconfig.Guard
	ticket       pluginconfig.Ticket
	accounts     map[int64]pluginconfig.Account
	order        []int64
	configModels map[string]struct{}

	credentials *CredentialStore
	store       *Store
	prober      *prober
	proxies     ProxyProvider
	now         func() time.Time
	logf        func(string, ...any)
	jitter      func(time.Duration) time.Duration
	slots       chan struct{}

	observedMu sync.Mutex
	observed   map[int64]map[string]time.Time

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
	running   atomic.Bool

	loopsMu sync.Mutex
	loops   map[poolKey]context.CancelFunc
	loopWG  sync.WaitGroup
}

// New 按一份已生效的配置创建票池管理器。
func New(options Options) *Manager {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	logf := options.Log
	if logf == nil {
		logf = func(string, ...any) {}
	}
	jitter := options.Jitter
	if jitter == nil {
		jitter = defaultJitter
	}
	registry := options.Registry
	if registry == nil {
		registry = NewRegistry()
	}

	guard := options.Config.OverloadGuard
	accounts := make(map[int64]pluginconfig.Account)
	order := make([]int64, 0)
	for _, account := range options.Config.EnabledAccounts() {
		accounts[account.AccountID] = account
		order = append(order, account.AccountID)
	}
	configModels := make(map[string]struct{}, len(guard.TicketPool.Models))
	for _, model := range guard.TicketPool.Models {
		configModels[model] = struct{}{}
	}

	return &Manager{
		guard:        guard,
		ticket:       guard.TicketPool,
		accounts:     accounts,
		order:        order,
		configModels: configModels,
		credentials:  registry.Credentials(),
		store:        registry.Tickets(),
		proxies:      options.Proxies,
		now:          now,
		logf:         logf,
		jitter:       jitter,
		slots:        make(chan struct{}, maxConcurrentProbes),
		observed:     make(map[int64]map[string]time.Time),
		loops:        make(map[poolKey]context.CancelFunc),
		prober: &prober{
			clients: options.Clients,
			config:  guard.TicketPool,
			// 撞票读的是网关回给我们的头，它固定叫 x-codex-turn-state；
			// header_name 说的是往上游注入时用什么名字，两者不是一回事。
			header: pluginconfig.DefaultHeaderName,
		},
	}
}

// Start 启动后台对账与补池循环。过载防护关闭或没有接管任何账号时不会启动。
func (m *Manager) Start(parent context.Context) {
	if !m.guard.Enabled || len(m.accounts) == 0 {
		return
	}
	m.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		m.cancel = cancel
		m.done = make(chan struct{})
		m.running.Store(true)
		go m.supervise(ctx)
	})
}

// Stop 停止所有循环并等待它们退出。
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
	})
	if m.done != nil {
		<-m.done
	}
	m.running.Store(false)
}

// --- transport.ValueSource ---

// Enabled 表示账号是否被过载防护接管。
func (m *Manager) Enabled(accountID int64) bool {
	_, ok := m.accounts[accountID]
	return ok
}

// Ready 表示账号已有至少一张有效票（不保证是某个具体模型的）。
func (m *Manager) Ready(accountID int64) bool {
	return m.store.Ready(accountID, m.now())
}

// Value 返回该模型当前可注入的 state。
func (m *Manager) Value(accountID int64, model string) (string, bool) {
	if !m.Enabled(accountID) {
		return "", false
	}
	return m.store.Pick(accountID, model, m.now())
}

// Observe 记录账号实际用到的模型；配置里没写的模型也能因此获得自己的池。
func (m *Manager) Observe(accountID int64, model string) {
	if !m.ticket.FollowObservedModels || !m.Enabled(accountID) || !trackableModel(model) {
		return
	}
	if _, known := m.configModels[model]; known {
		return
	}
	now := m.now()
	m.observedMu.Lock()
	defer m.observedMu.Unlock()
	models := m.observed[accountID]
	if models == nil {
		models = make(map[string]time.Time, 4)
		m.observed[accountID] = models
	}
	if _, exists := models[model]; !exists && len(models) >= maxObservedModels {
		evictOldest(models)
	}
	models[model] = now
}

// Harvest 从过路请求里采集撞票凭据。只对被接管的账号生效。
//
// 这是插件不依赖外部服务端的关键：OAuth access token 从真实请求里来，
// 只留在内存里，不落盘、不进日志、不出现在任何对外输出中。代价是一个账号
// 必须先有一个真实请求经过，它的票池才会开始预热。
func (m *Manager) Harvest(accountID int64, header http.Header, rawURL string) {
	if !m.Enabled(accountID) {
		return
	}
	cred, ok := harvestCredential(header, rawURL, m.now())
	if !ok {
		return
	}
	if m.credentials.Observe(accountID, cred) {
		m.logf("账号 %d 已从过路请求获取到撞票凭据，开始维护票池", accountID)
	}
}

// --- 后台循环 ---

func (m *Manager) supervise(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(superviseInterval)
	defer ticker.Stop()
	for {
		m.reconcile(ctx)
		select {
		case <-ctx.Done():
			m.loopWG.Wait()
			return
		case <-ticker.C:
		}
	}
}

// reconcile 让运行中的补池循环与「应该有池的 (账号 × 模型)」保持一致。
func (m *Manager) reconcile(ctx context.Context) {
	now := m.now()
	desired := make(map[poolKey]struct{})
	for _, accountID := range m.order {
		// 没有凭据就撞不了票，也不该在看板上显示成"正在维护"。
		if !m.credentials.Has(accountID) {
			continue
		}
		keep := make(map[string]struct{})
		for _, model := range m.modelsFor(accountID) {
			desired[poolKey{accountID, model}] = struct{}{}
			keep[model] = struct{}{}
		}
		// 不再维护、也已经没有有效票的池（过期的自动跟踪模型、配置里删掉的模型）
		// 从内存与看板里收走；还有票的池留到票过期，免得保存一次配置就丢一批票。
		m.store.Prune(accountID, keep, now)
	}

	m.loopsMu.Lock()
	defer m.loopsMu.Unlock()
	for key, cancel := range m.loops {
		if _, keep := desired[key]; keep {
			continue
		}
		cancel()
		delete(m.loops, key)
	}
	for key := range desired {
		if _, exists := m.loops[key]; exists {
			continue
		}
		loopCtx, cancel := context.WithCancel(ctx)
		m.loops[key] = cancel
		m.store.Touch(key.account, key.model)
		m.loopWG.Add(1)
		go func(key poolKey) {
			defer m.loopWG.Done()
			m.runPool(loopCtx, key)
		}(key)
	}
}

// modelsFor 返回某账号当前要维护的模型：配置里写死的 + 最近真实用过的。
func (m *Manager) modelsFor(accountID int64) []string {
	models := append([]string(nil), m.ticket.Models...)
	if !m.ticket.FollowObservedModels {
		return models
	}
	cutoff := m.now().Add(-observedModelTTL)
	m.observedMu.Lock()
	for model, seen := range m.observed[accountID] {
		if seen.Before(cutoff) {
			delete(m.observed[accountID], model)
			continue
		}
		models = append(models, model)
	}
	m.observedMu.Unlock()
	sort.Strings(models)
	return models
}

func (m *Manager) runPool(ctx context.Context, key poolKey) {
	if !sleepCtx(ctx, m.jitter(startupJitter)) {
		return
	}
	interval := time.Duration(m.ticket.CheckIntervalSeconds) * time.Second
	for {
		m.Refill(ctx, key.account, key.model)
		if !sleepCtx(ctx, interval) {
			return
		}
	}
}

// Refill 对单个池跑一轮补池决策与撞票，返回新增票数。
//
// 决策表见 Store.Plan；撞不到满血票时会换未用过的代理重试 retry_rounds 轮。
func (m *Manager) Refill(ctx context.Context, accountID int64, model string) int {
	cred, ok := m.credentials.Get(accountID)
	if !ok {
		m.store.NoteRound(accountID, model, "no-credential", 0, nil,
			"还没有从过路请求中采到该账号的凭据", m.now())
		return 0
	}

	settings := m.ticket
	plan := m.store.Plan(accountID, model, settings.PoolSize,
		settings.RefillThresholdSeconds, m.now())
	if plan.Need == 0 {
		m.store.NoteRound(accountID, model, plan.Reason, 0, nil, "", m.now())
		return 0
	}

	params := StoreParams{
		Cap:            settings.PoolSize,
		TTLSeconds:     settings.TicketTTLSeconds,
		StaggerSeconds: settings.TicketStaggerSeconds,
		MinTTLSeconds:  pluginconfig.MinTicketTTLSeconds,
		Size:           settings.PoolSize,
	}
	if plan.Replace {
		// 一换一：允许临时超存一张，StoreRound 末尾会把最快过期的那张裁掉。
		params.Cap = settings.PoolSize + 1
	}
	perRound := settings.ProxiesPerRound
	if plan.Need < perRound {
		perRound = plan.Need
	}

	grades := make(map[Grade]int)
	tried := make(map[string]struct{})

	result := m.round(ctx, cred, model, perRound, tried)
	mergeGrades(grades, result.grades)
	markTried(tried, result.attempted)
	detail := result.detail
	added := m.store.StoreRound(accountID, model, result.records, params, m.now())

	for attempt := 0; added == 0 && attempt < settings.RetryRounds && ctx.Err() == nil; attempt++ {
		more := m.round(ctx, cred, model, perRound, tried)
		mergeGrades(grades, more.grades)
		markTried(tried, more.attempted)
		if len(more.records) == 0 {
			// 连 state 都没拿到，说明不是出口的问题，换代理也没意义。
			break
		}
		if more.detail != "" {
			detail = more.detail
		}
		added += m.store.StoreRound(accountID, model, more.records, params, m.now())
	}

	if added > 0 {
		detail = ""
	}
	m.store.NoteRound(accountID, model, plan.Reason, added, grades, detail, m.now())
	if added > 0 {
		m.logf("账号 %d 模型 %s 补入 %d 张票（%s，%s）",
			accountID, model, added, plan.Reason, formatGrades(grades))
	}
	return added
}

// roundResult 是一轮撞票（直连 + 若干代理）的汇总。
type roundResult struct {
	// records 是带 state 的记录，已按 (分级, 长度降序) 排好。
	records []record
	grades  map[Grade]int
	// attempted 是本轮用过的代理地址，用于下一轮换代理。
	attempted []string
	detail    string
}

// round 并发跑一轮探针：直连一发（可关）+ 最多 proxyCount 个代理各一发。
//
// 参考实现是串行的，单轮最坏要等 5×probe_timeout；这里并发跑，
// 全局在途量由 maxConcurrentProbes 兜住。结果排序与顺序无关。
func (m *Manager) round(ctx context.Context, cred Credential, model string,
	proxyCount int, skip map[string]struct{}) roundResult {

	var targets []string
	if m.ticket.IncludeDirect {
		targets = append(targets, "")
	}
	if proxyCount > 0 && m.proxies != nil {
		targets = append(targets, m.proxies.Lease(proxyCount, skip)...)
	}
	out := roundResult{grades: make(map[Grade]int)}
	if len(targets) == 0 {
		out.detail = "本轮没有可用的撞票出口（直连已关闭，代理库里也没有可用或还没试过的代理）"
		return out
	}

	results := make([]record, len(targets))
	var group sync.WaitGroup
	for index, target := range targets {
		group.Add(1)
		go func(index int, target string) {
			defer group.Done()
			if !m.acquire(ctx) {
				results[index] = record{proxy: target, grade: GradeError, detail: "已取消"}
				return
			}
			defer m.release()
			results[index] = m.prober.probe(ctx, cred, model, target)
		}(index, target)
	}
	group.Wait()

	// 循环被取消（账号取消接管、模型不再跟踪、配置切换）时探针会以 error 收场，
	// 那不是出口的问题，不能记到代理头上。
	cancelled := ctx.Err() != nil
	for _, item := range results {
		out.grades[item.grade]++
		if out.detail == "" && item.detail != "" {
			out.detail = item.detail
		}
		if item.proxy == "" {
			continue
		}
		out.attempted = append(out.attempted, item.proxy)
		if m.proxies != nil {
			// 正常收到响应（含 4xx）说明这个出口是通的，只有连响应都没拿到才记失败。
			ok := item.grade != GradeError
			if ok || !cancelled {
				m.proxies.Note(item.proxy, ok)
			}
		}
	}
	out.records = sortRecords(results)
	if out.detail == "" {
		// 没有任何一个出口报错，却也可能一张满血票都没撞到（例如全是 312）。
		// 把各出口的分级摘要留给看板，管理员能一眼分清「长度不符」和「被挡」。
		out.detail = summarizeRecords(results)
	}
	return out
}

func (m *Manager) acquire(ctx context.Context) bool {
	select {
	case m.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *Manager) release() {
	<-m.slots
}

// Status 汇总所有账号的票池状态，供健康检查与配置页看板使用。
func (m *Manager) Status() Status {
	now := m.now()
	status := Status{Running: m.running.Load(), Accounts: make([]AccountStatus, 0, len(m.order))}
	for _, accountID := range m.order {
		account := AccountStatus{
			AccountID:         accountID,
			Note:              m.accounts[accountID].Note,
			CredentialSeconds: -1,
		}
		if cred, ok := m.credentials.Get(accountID); ok {
			account.HasCredential = true
			account.CredentialSeconds = int(now.Sub(cred.UpdatedAt) / time.Second)
		}
		account.Models = m.withConfiguredModels(m.store.ModelStatuses(accountID, m.ticket.PoolSize, now))
		for _, model := range account.Models {
			status.Valid += model.Valid
			status.Wanted += m.ticket.PoolSize
		}
		status.Accounts = append(status.Accounts, account)
	}
	return status
}

// withConfiguredModels 让配置里写了但还没建池的模型也出现在看板上（显示 0/N），
// 否则管理员会以为自己配的模型被忽略了。
func (m *Manager) withConfiguredModels(models []ModelStatus) []ModelStatus {
	present := make(map[string]struct{}, len(models))
	for _, model := range models {
		present[model.Model] = struct{}{}
	}
	for _, model := range m.ticket.Models {
		if _, exists := present[model]; exists {
			continue
		}
		models = append(models, ModelStatus{
			Model:             model,
			Size:              m.ticket.PoolSize,
			LastRefillSeconds: -1,
		})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].Model < models[j].Model })
	return models
}

func mergeGrades(into, from map[Grade]int) {
	for grade, count := range from {
		into[grade] += count
	}
}

func markTried(tried map[string]struct{}, addrs []string) {
	for _, addr := range addrs {
		tried[addr] = struct{}{}
	}
}

// formatGrades 给日志用的稳定顺序输出，例如 "healthy=1 downgraded=3"。
func formatGrades(grades map[Grade]int) string {
	names := make([]string, 0, len(grades))
	for grade := range grades {
		names = append(names, string(grade))
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", name, grades[Grade(name)]))
	}
	return strings.Join(parts, " ")
}

// trackableModel 挡掉不适合建池的模型名：请求体是外部输入，池键不能由它随意膨胀。
// 字符集与长度和配置里 models 的校验规则完全一致，嗅探到的名字同样会进探针请求体与日志。
func trackableModel(model string) bool {
	return pluginconfig.ValidModelName(model)
}

func evictOldest(models map[string]time.Time) {
	oldest := ""
	var oldestSeen time.Time
	for model, seen := range models {
		if oldest == "" || seen.Before(oldestSeen) {
			oldest, oldestSeen = model, seen
		}
	}
	if oldest != "" {
		delete(models, oldest)
	}
}

func defaultJitter(window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(window)))
}

// sleepCtx 在可取消的前提下等待，返回 false 表示已取消。
func sleepCtx(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
