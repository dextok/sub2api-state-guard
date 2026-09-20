// Package runtime 实现宿主要求的 TransportPlugin gRPC 服务。
//
// 七个方法的分工见宿主的 docs/PLUGIN_DEVELOPMENT.md：GetInfo/Health 只报身份与进程状态，
// ValidateConfig/ApplyConfig 复用 pluginconfig 的同一套解析与校验，TestConfig 返回
// 一份只读的实时票况快照，Forward 承载真正的出站流量；InitHostServices 保持
// Unimplemented——本插件不用宿主的 KV 存储，宿主会静默跳过。
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
	"github.com/dextok/sub2api-state-guard/internal/proxypool"
	"github.com/dextok/sub2api-state-guard/internal/ticketpool"
	"github.com/dextok/sub2api-state-guard/internal/transport"
)

// Capability 是本插件声明的唯一能力，必须与清单里的 capabilities 完全一致。
const Capability = "openai.oauth.outbound_transport.v1"

const (
	// transportGracePeriod 是旧连接池的宽限期。
	//
	// 配置切换时立刻关闭旧池会让"已经进入 Forward、但还没取到客户端"的请求
	// 撞上"连接池已关闭"，所以旧池延迟回收，旧的后台循环则立即停摆。
	transportGracePeriod = 5 * time.Second

	// maxStatusLines 限制 TestConfig 返回的账号明细行数，避免消息过长。
	maxStatusLines = 20

	// StatusSentinel 之后跟着一行紧凑 JSON，是 config.test 面向老宿主的看板通道。
	//
	// sub2api ≥ 0.2.7 的 TestConfigResponse 有 status_json 字段，看板数据直接放那儿；
	// 更老的宿主只转发 success/message/latency_ms 三个标量，结构化数据只能搭在
	// message 里。两处内容完全一致，UI 解析完这一行后会把它从展示文本中去掉。
	StatusSentinel = "<<<overload-guard-status>>>"
)

// Identity 是打包时通过 ldflags 注入的插件身份，必须与清单中的 id/version 一致，
// 否则宿主会在启动阶段判定「插件运行时信息与已校验清单不一致」并杀掉进程。
type Identity struct {
	PluginID      string
	PluginVersion string
}

// state 是一份已生效配置对应的全部运行时对象，整体原子替换。
type state struct {
	config      pluginconfig.Config
	forwarder   *transport.Forwarder
	capturePool *transport.Pool
	proxies     *proxypool.Pool
	tickets     *ticketpool.Manager
	ctx         context.Context
	cancel      context.CancelFunc
}

// start 启动这份状态的后台撞票循环。它与 buildState 分开，是为了让 ApplyConfig
// 能在「旧循环彻底停下、旧账号的票被抹掉」之后再拉起新循环。
func (s *state) start() {
	if s.config.OverloadGuard.Enabled && len(s.activeAccounts()) > 0 {
		s.tickets.Start(s.ctx)
	}
}

// activeAccounts 返回这份配置接管的账号集合。
func (s *state) activeAccounts() map[int64]struct{} {
	active := make(map[int64]struct{})
	for _, account := range s.config.EnabledAccounts() {
		active[account.AccountID] = struct{}{}
	}
	return active
}

// retire 停用一份旧状态：后台循环立即停摆（并等在途探针结束），连接池延迟回收。
func (s *state) retire(grace time.Duration) {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.tickets.Stop()
	if grace <= 0 {
		s.closePools()
		return
	}
	time.AfterFunc(grace, s.closePools)
}

func (s *state) closePools() {
	if s == nil {
		return
	}
	s.forwarder.Close()
	s.capturePool.Close()
}

// Server 实现 pluginv1.TransportPluginServer。
//
// 请求路径只做一次 atomic 读，配置切换与请求转发互不阻塞。
type Server struct {
	pluginv1.UnimplementedTransportPluginServer

	identity Identity
	registry *ticketpool.Registry
	logf     func(format string, args ...any)

	// applyMu 串行化配置切换，保证「构建新状态 → 换入 → 停用旧状态」不会交错。
	applyMu sync.Mutex
	active  atomic.Pointer[state]
}

// New 创建服务实例，并以全默认配置启动（此时没有任何账号被接管，纯透传）。
//
// 宿主在启用插件后才会下发配置，但 Forward 有可能先到，所以初始状态必须可用。
func New(identity Identity, logf func(format string, args ...any)) *Server {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	server := &Server{
		identity: identity,
		registry: ticketpool.NewRegistry(),
		logf:     logf,
	}
	initial := server.buildState(pluginconfig.Default())
	server.active.Store(initial)
	initial.start()
	return server
}

// Close 释放当前状态，仅供测试与优雅退出使用。
func (s *Server) Close() {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if previous := s.active.Swap(nil); previous != nil {
		previous.retire(0)
	}
}

func (s *Server) GetInfo(_ context.Context, _ *pluginv1.GetInfoRequest) (*pluginv1.GetInfoResponse, error) {
	return &pluginv1.GetInfoResponse{
		PluginId:            s.identity.PluginID,
		PluginVersion:       s.identity.PluginVersion,
		ProtocolVersion:     pluginv1.ProtocolVersion,
		TransportApiVersion: pluginv1.TransportAPIVersion,
		Capabilities:        []string{Capability},
	}, nil
}

// Health 报进程状态与票池水位，不探网络：宿主会周期调用它。
//
// 票池状态全在内存里，取它不产生任何出站流量，所以这里可以顺带把水位报出来。
//
// StatusJson 是同一份看板快照。宿主（sub2api ≥ 0.2.7）把 UI Bridge 的
// plugin.status 动词直接映射到 Health：那条通道免二次验证、也不弹宿主提示，
// 配置页的实时看板就靠它轮询。所以这里必须保持「被动」——只读内存状态，
// 不应用配置、不发任何请求。
func (s *Server) Health(_ context.Context, _ *pluginv1.HealthRequest) (*pluginv1.HealthResponse, error) {
	current := s.state()
	if current == nil {
		return &pluginv1.HealthResponse{Healthy: false, Message: "插件尚未就绪"}, nil
	}
	// Health 拿不到「宿主存的那份配置」，没法判断是否已应用，所以 synced 留空。
	return &pluginv1.HealthResponse{
		Healthy:    true,
		Message:    healthMessage(current),
		StatusJson: statusJSON(current, nil),
	}, nil
}

func healthMessage(current *state) string {
	guard := current.config.OverloadGuard
	if !guard.Enabled {
		return "运行中：过载防护已关闭，仅透传。"
	}
	tickets := current.tickets.Status()
	if len(tickets.Accounts) == 0 {
		return "运行中：还没有账号被接管。"
	}
	withCredential := 0
	for _, account := range tickets.Accounts {
		if account.HasCredential {
			withCredential++
		}
	}
	proxies := current.proxies.Status()
	return fmt.Sprintf("运行中：接管 %d 个账号（%d 个已取到凭据），票池 %d/%d 张，代理库 %d 条。",
		len(tickets.Accounts), withCredential, tickets.Valid, tickets.Wanted, proxies.Total)
}

// ValidateConfig 严格解析配置并回传完整规范化结果。
//
// 校验失败走 valid=false 而不是 gRPC 错误：宿主会把 message 原样展示给管理员。
func (s *Server) ValidateConfig(_ context.Context, request *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	config, err := pluginconfig.Parse(request.GetConfigJson())
	if err != nil {
		return &pluginv1.ValidateConfigResponse{Valid: false, Message: err.Error()}, nil
	}
	normalized, err := config.Marshal()
	if err != nil {
		return &pluginv1.ValidateConfigResponse{Valid: false, Message: "序列化规范化配置失败: " + err.Error()}, nil
	}
	return &pluginv1.ValidateConfigResponse{
		Valid:                true,
		Message:              describeConfig(config),
		NormalizedConfigJson: normalized,
	}, nil
}

// ApplyConfig 原子切换配置。
//
// 失败时保留旧配置继续服务；成功时已经撞到的票与已采到的凭据不会丢——
// 它们记在跨配置存活的 ticketpool.Registry 上，只有被取消接管的账号会被清掉。
func (s *Server) ApplyConfig(_ context.Context, request *pluginv1.ApplyConfigRequest) (*pluginv1.ApplyConfigResponse, error) {
	config, err := pluginconfig.Parse(request.GetConfigJson())
	if err != nil {
		return &pluginv1.ApplyConfigResponse{Applied: false, Message: err.Error()}, nil
	}

	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	next := s.buildState(config)
	previous := s.active.Swap(next)
	// 顺序很重要：先让旧循环彻底停下（Stop 会等在途探针结束），再抹掉被取消接管账号的
	// 凭据与票，最后才拉起新循环。反过来的话，旧循环里在途的探针会把刚抹掉的票写回去，
	// 而那个账号的 access token 也会重新回到内存里。
	previous.retire(transportGracePeriod)
	s.registry.Retain(next.activeAccounts())
	next.start()

	s.logf("已应用配置：接管 %d 个账号", len(config.EnabledAccounts()))
	return &pluginv1.ApplyConfigResponse{Applied: true, Message: describeConfig(config)}, nil
}

// dashboard 是配置页实时看板的数据结构。两条通道下发同一份内容：
// plugin.status（Health.status_json）与 config.test（status_json，并兼容老宿主
// 跟在 StatusSentinel 之后的那行紧凑 JSON）。
//
// 里面只有计数与时间：state 原文、OAuth 凭据、代理完整地址一律不出现。
type dashboard struct {
	Guard   guardSummary      `json:"guard"`
	Tickets ticketpool.Status `json:"tickets"`
	Proxies proxypool.Status  `json:"proxies"`
	// Synced 为假表示传入的配置与当前生效配置不一致（通常是保存后还没应用）。
	// 为空表示这次调用没有可比对的配置（plugin.status 走 Health，宿主不带配置）。
	Synced *bool `json:"synced,omitempty"`
}

type guardSummary struct {
	Enabled      bool   `json:"enabled"`
	HeaderName   string `json:"header_name"`
	Accounts     int    `json:"accounts"`
	PoolSize     int    `json:"pool_size"`
	TargetLength int    `json:"target_state_length"`
}

// snapshot 组装看板快照：只读内存状态，不发任何请求。
func snapshot(current *state, tickets ticketpool.Status, proxies proxypool.Status, synced *bool) dashboard {
	guard := current.config.OverloadGuard
	return dashboard{
		Guard: guardSummary{
			Enabled:      guard.Enabled,
			HeaderName:   guard.HeaderName,
			Accounts:     len(tickets.Accounts),
			PoolSize:     guard.TicketPool.PoolSize,
			TargetLength: guard.TicketPool.TargetStateLength,
		},
		Tickets: tickets,
		Proxies: proxies,
		Synced:  synced,
	}
}

// statusJSON 把快照编成 JSON；编码失败就返回空串，状态读不到总好过让调用失败。
func statusJSON(current *state, synced *bool) string {
	encoded, err := json.Marshal(snapshot(current, current.tickets.Status(), current.proxies.Status(), synced))
	if err != nil {
		return ""
	}
	return string(encoded)
}

// TestConfig 返回一份只读的实时票况快照，不产生任何出站流量。
//
// 插件自建票池之后，这里已经没有「外部服务端」可探测了，所以它同时充当配置页
// 「刷新状态」按钮的数据源：快照放在 status_json 里，另外仍跟一行
// StatusSentinel + JSON，好让没有 status_json 字段的老宿主也能读到。
//
// 实时看板的自动轮询走的不是这里，而是免二次验证、不弹提示的 plugin.status
// （见 Health）：宿主会把 config.test 当作「测试」这种敏感动作对待。
func (s *Server) TestConfig(_ context.Context, request *pluginv1.TestConfigRequest) (*pluginv1.TestConfigResponse, error) {
	started := time.Now()
	config, err := pluginconfig.Parse(request.GetConfigJson())
	if err != nil {
		return &pluginv1.TestConfigResponse{
			Success:   false,
			Message:   err.Error(),
			LatencyMs: time.Since(started).Milliseconds(),
		}, nil
	}

	current := s.state()
	if current == nil {
		return &pluginv1.TestConfigResponse{
			Success:   false,
			Message:   "插件尚未就绪",
			LatencyMs: time.Since(started).Milliseconds(),
		}, nil
	}

	tickets := current.tickets.Status()
	proxies := current.proxies.Status()
	synced := sameConfig(current.config, config)

	success, lines := summarize(current.config, tickets, proxies, synced)
	payload := snapshot(current, tickets, proxies, &synced)
	encoded, encodeErr := json.Marshal(payload)
	if encodeErr == nil {
		lines = append(lines, StatusSentinel+string(encoded))
	}
	return &pluginv1.TestConfigResponse{
		Success:    success,
		Message:    strings.Join(lines, "\n"),
		LatencyMs:  time.Since(started).Milliseconds(),
		StatusJson: string(encoded),
	}, nil
}

// summarize 把状态写成给人看的几行，并判断整体是否「正常」。
//
// 判定刻意宽松：还没预热完不算失败，只有「已经撞过至少一轮、却一张票都没有」
// 才算真出了问题——那通常是凭据拿错了、代理全挂了或者账号被限。
func summarize(config pluginconfig.Config, tickets ticketpool.Status,
	proxies proxypool.Status, synced bool) (bool, []string) {

	guard := config.OverloadGuard
	lines := make([]string, 0, maxStatusLines+4)
	if !synced {
		lines = append(lines, "提示：这份配置还没有被应用（请先保存），下面展示的是当前生效配置的状态。")
	}
	if !guard.Enabled {
		return true, append(lines, "过载防护总开关已关闭：插件只做透传，不注入任何头。")
	}
	if len(tickets.Accounts) == 0 {
		return true, append(lines, "过载防护已开启，但还没有账号被接管，不会有任何撞票流量。")
	}

	lines = append(lines, fmt.Sprintf("票池：%d/%d 张有效（%d 个账号，每个模型目标 %d 张，满血长度 %d）。",
		tickets.Valid, tickets.Wanted, len(tickets.Accounts),
		guard.TicketPool.PoolSize, guard.TicketPool.TargetStateLength))
	lines = append(lines, formatProxyLine(guard.ProxyPool, proxies, guard.TicketPool.IncludeDirect))

	waiting := 0
	probed := false
	for _, account := range tickets.Accounts {
		if !account.HasCredential {
			waiting++
		}
		for _, model := range account.Models {
			if model.LastRefillSeconds >= 0 {
				probed = true
			}
		}
	}
	if waiting > 0 {
		lines = append(lines, fmt.Sprintf("有 %d 个账号还没有凭据：插件从过路请求里取 OAuth 令牌，"+
			"这些账号要先有一个真实请求经过，票池才会开始预热。", waiting))
	}

	for index, account := range tickets.Accounts {
		if index == maxStatusLines {
			lines = append(lines, fmt.Sprintf("……另有 %d 个账号未展示。", len(tickets.Accounts)-maxStatusLines))
			break
		}
		lines = append(lines, formatAccount(account))
	}

	success := true
	if probed && tickets.Valid == 0 {
		success = false
		lines = append(lines, "已经撞过至少一轮但一张满血票都没有：请检查代理库是否可用、"+
			"账号是否被限流，或把「满血票长度」调整为上游当前实际返回的长度。")
	}
	return success, lines
}

func formatProxyLine(config pluginconfig.Proxy, status proxypool.Status, includeDirect bool) string {
	if !config.Enabled {
		if includeDirect {
			return "代理库：已关闭，撞票只走直连。"
		}
		return "代理库：已关闭，且直连也未开启——当前没有任何撞票出口。"
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("代理库：%d 条", status.Total))
	if schemes := countSchemes(status); schemes != "" {
		builder.WriteString("（" + schemes + "）")
	}
	success, fail := 0, 0
	for _, item := range status.Entries {
		success += item.Success
		fail += item.Fail
	}
	builder.WriteString(fmt.Sprintf("，撞票成功 %d 次、失败 %d 次。", success, fail))
	return builder.String()
}

// countSchemes 把各协议的条数汇成 "socks5 3 / http 1"，顺序固定以便对比两次输出。
func countSchemes(status proxypool.Status) string {
	counts := make(map[string]int, 4)
	for _, item := range status.Entries {
		counts[item.Scheme]++
	}
	parts := make([]string, 0, len(counts))
	for _, scheme := range []string{
		pluginconfig.ProxySchemeSOCKS5,
		pluginconfig.ProxySchemeSOCKS5H,
		pluginconfig.ProxySchemeHTTP,
		pluginconfig.ProxySchemeHTTPS,
	} {
		if counts[scheme] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", scheme, counts[scheme]))
		}
	}
	return strings.Join(parts, " / ")
}

func formatAccount(account ticketpool.AccountStatus) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("账号 %d", account.AccountID))
	if account.Note != "" {
		builder.WriteString("（" + account.Note + "）")
	}
	if !account.HasCredential {
		builder.WriteString("：等待第一个真实请求以获取凭据")
		return builder.String()
	}
	builder.WriteString(fmt.Sprintf("：凭据 %s 前采集", truncateDuration(time.Duration(account.CredentialSeconds)*time.Second)))
	if len(account.Models) == 0 {
		builder.WriteString("；还没有任何模型的票池")
		return builder.String()
	}
	parts := make([]string, 0, len(account.Models))
	for _, model := range account.Models {
		part := fmt.Sprintf("%s %d/%d", model.Model, model.Valid, model.Size)
		if model.Valid > 0 {
			part += fmt.Sprintf("（最长 %s）", truncateDuration(time.Duration(model.FreshestSeconds)*time.Second))
		} else if model.Reason != "" {
			part += "（" + model.Reason + "）"
		}
		parts = append(parts, part)
	}
	builder.WriteString("；" + strings.Join(parts, "，"))
	return builder.String()
}

func truncateDuration(value time.Duration) time.Duration {
	return value.Truncate(time.Second)
}

// sameConfig 判断两份配置是否等价。规范化之后同一份配置的 JSON 是稳定的。
func sameConfig(left, right pluginconfig.Config) bool {
	leftJSON, leftErr := left.Marshal()
	rightJSON, rightErr := right.Marshal()
	if leftErr != nil || rightErr != nil {
		return false
	}
	return string(leftJSON) == string(rightJSON)
}

// state 返回当前生效状态。Forward 路径的唯一读取点。
func (s *Server) state() *state {
	return s.active.Load()
}

// buildState 用一份已校验配置构建运行时对象。后台循环由 state.start 另行启动。
func (s *Server) buildState(config pluginconfig.Config) *state {
	capturePool := transport.NewPool(capturePoolConfig(config))
	proxies := proxypool.New(config.OverloadGuard.ProxyPool, time.Now)
	tickets := ticketpool.New(ticketpool.Options{
		Config:   config,
		Registry: s.registry,
		Clients:  capturePool,
		Proxies:  proxies,
		Log:      s.logf,
	})

	ctx, cancel := context.WithCancel(context.Background())
	return &state{
		config:      config,
		forwarder:   transport.New(config, tickets),
		capturePool: capturePool,
		proxies:     proxies,
		tickets:     tickets,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// capturePoolConfig 给撞票探针单独一套连接参数。
//
// 与出站请求分池，是因为撞票流量不该挤占上游连接池；proxy_mode 强制为 host，
// 否则管理员把它关成 disabled 时，撞票就再也用不上代理出口了——
// 那个开关说的是「要不要沿用宿主给上游请求下发的代理」，与撞票出口无关。
func capturePoolConfig(config pluginconfig.Config) pluginconfig.Config {
	clone := config.Clone()
	clone.ProxyMode = pluginconfig.ProxyModeHost
	clone.ExtraHeaders = nil
	return clone
}

func describeConfig(config pluginconfig.Config) string {
	guard := config.OverloadGuard
	if !guard.Enabled {
		return "过载防护已关闭：仅接管出站传输，不注入任何头。"
	}
	enabled := len(config.EnabledAccounts())
	if enabled == 0 {
		return fmt.Sprintf("过载防护已开启，但还没有账号被接管（头 %s 不会被注入）。", guard.HeaderName)
	}
	pool := guard.TicketPool
	return fmt.Sprintf("过载防护已开启：%d 个账号自建票池并按模型注入 %s；每个模型 %d 张、有效期 %d 秒、"+
		"满血长度 %d，每 %d 秒检查一次；模型 %s；覆盖模式 %s，无可用值时 %s，模型未匹配时 %s。",
		enabled, guard.HeaderName, pool.PoolSize, pool.TicketTTLSeconds, pool.TargetStateLength,
		pool.CheckIntervalSeconds, describeModels(pool), guard.OverrideMode,
		guard.OnUnavailable, guard.OnModelUnmatched)
}

func describeModels(pool pluginconfig.Ticket) string {
	models := append([]string(nil), pool.Models...)
	sort.Strings(models)
	text := joinLimited(models, 8)
	if pool.FollowObservedModels {
		return text + "（并自动跟踪请求里出现的新模型）"
	}
	return text
}

func joinLimited(values []string, limit int) string {
	if len(values) == 0 {
		return "无"
	}
	if len(values) > limit {
		return strings.Join(values[:limit], "、") + fmt.Sprintf("…… 另有 %d 个", len(values)-limit)
	}
	return strings.Join(values, "、")
}
