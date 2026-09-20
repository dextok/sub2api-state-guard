package ticketpool

import (
	"sort"
	"sync"
	"time"
)

// ticket 是池里的一张算力票。state 只在内存中流转，不落盘也不进日志。
type ticket struct {
	state     string
	length    int
	createdAt time.Time
	expiresAt time.Time
	// proxy 记录这张票是从哪个出口撞到的，空表示直连。只用于诊断。
	proxy string
}

// poolKey 是票池的主键：票按 (账号 × 模型) 分池，绝不跨模型复用。
type poolKey struct {
	account int64
	model   string
}

// poolState 是单个池的内容与最近一轮补池的结果。
type poolState struct {
	tickets []ticket

	lastRefillAt time.Time
	lastReason   string
	lastAdded    int
	lastGrades   map[Grade]int
	lastError    string
	hits         int64
	misses       int64
}

// Store 是 (账号 × 模型) 票池的内存表，跨 ApplyConfig 存活。
//
// 配置改动不该把已经撞到的票扔掉——它们还有几十分钟有效期，重撞既慢又费流量。
type Store struct {
	mu    sync.Mutex
	pools map[poolKey]*poolState
}

// NewStore 创建空票池表。
func NewStore() *Store {
	return &Store{pools: make(map[poolKey]*poolState)}
}

// Pick 取 (账号, 模型) 池里最新的一张有效票。
//
// 取最新而不是最快过期的：新票剩余有效期最长，能撑过更长的流式请求。
// 票不会被消费，有效期内可重复注入。targetLength 非 0 时只认长度相符的票，
// 这样改了满血长度之后，没有循环在维护的池也不会继续注入旧口径的票。
func (s *Store) Pick(accountID int64, model string, targetLength int, now time.Time) (string, bool) {
	if model == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pool, exists := s.pools[poolKey{accountID, model}]
	if !exists {
		return "", false
	}
	var best *ticket
	for index := range pool.tickets {
		item := &pool.tickets[index]
		if !usable(item, now, targetLength) {
			continue
		}
		if best == nil || item.createdAt.After(best.createdAt) {
			best = item
		}
	}
	if best == nil {
		pool.misses++
		return "", false
	}
	pool.hits++
	return best.state, true
}

// Ready 表示账号至少有一张仍在有效期内、长度也相符的票（不保证是某个具体模型的）。
//
// 满血长度按模型配置，所以这里不能用一个数字扫全部池：每个池都要拿自己模型的口径来判。
func (s *Store) Ready(accountID int64, targetFor func(model string) int, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, pool := range s.pools {
		if key.account != accountID {
			continue
		}
		targetLength := targetFor(key.model)
		for index := range pool.tickets {
			if usable(&pool.tickets[index], now, targetLength) {
				return true
			}
		}
	}
	return false
}

// usable 判断一张票现在还能不能注入：没过期，且长度符合当前口径。
func usable(item *ticket, now time.Time, targetLength int) bool {
	if !now.Before(item.expiresAt) {
		return false
	}
	return targetLength == 0 || item.length == targetLength
}

// Valid 返回某个池里仍然有效的票数。
func (s *Store) Valid(accountID int64, model string, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return countValid(s.pools[poolKey{accountID, model}], now)
}

// Models 返回某个账号当前有池的模型列表（按名称排序）。
func (s *Store) Models(accountID int64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	models := make([]string, 0, len(s.pools))
	for key := range s.pools {
		if key.account == accountID {
			models = append(models, key.model)
		}
	}
	sort.Strings(models)
	return models
}

// Retain 丢弃不再被接管的账号的全部票。
func (s *Store) Retain(active map[int64]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.pools {
		if _, keep := active[key.account]; !keep {
			delete(s.pools, key)
		}
	}
}

// Prune 收走某账号名下既不再维护、也没有有效票的池。
//
// 自动跟踪的模型过期、配置里删掉的模型，它们的池不再有循环补票；剩下的票在有效期内
// 照常可用，等最后一张过期后整个池就该从内存与看板里消失——否则请求体里出现过的
// 每个模型名都会永久留下一张不再更新的卡片，Wanted 也会被它们虚增。
func (s *Store) Prune(accountID int64, keep map[string]struct{}, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, pool := range s.pools {
		if key.account != accountID {
			continue
		}
		if _, kept := keep[key.model]; kept {
			continue
		}
		if countValid(pool, now) > 0 {
			continue
		}
		delete(s.pools, key)
	}
}

// Plan 是一次补池决策的结果。
type Plan struct {
	// Reason 取值：empty / partial / cooldown / full / renew。
	Reason string
	// Need 是这一轮想补几张；0 表示不补。
	Need int
	// Replace 为真时允许临时超存一张，随后由 Trim 淘汰最快过期的那张（一换一）。
	Replace bool
	// Valid 是决策时池里的有效票数。
	Valid int
}

// Plan 按参考实现的决策表给出这一轮补不补、补几张：
//
//  1. 池内无票 → 无条件补满；
//  2. 池未满 → 距最近一张票生成不足 threshold 则冷却跳过，否则补齐差额；
//  3. 池已满 → 剩余有效期最长的一张也不足 threshold 才补，且补到后一换一。
//
// 同批入池的票过期时间已经错开，不会整池同时到期。
func (s *Store) Plan(accountID int64, model string, size, thresholdSeconds, targetLength int, now time.Time) Plan {
	threshold := time.Duration(thresholdSeconds) * time.Second

	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensureLocked(poolKey{accountID, model})
	purgeLocked(pool, now, targetLength)

	valid := len(pool.tickets)
	if valid == 0 {
		return Plan{Reason: "empty", Need: size, Valid: 0}
	}
	if valid < size {
		newest := pool.tickets[0].createdAt
		for _, item := range pool.tickets {
			if item.createdAt.After(newest) {
				newest = item.createdAt
			}
		}
		if now.Sub(newest) < threshold {
			return Plan{Reason: "cooldown", Need: 0, Valid: valid}
		}
		return Plan{Reason: "partial", Need: size - valid, Valid: valid}
	}
	freshest := pool.tickets[0].expiresAt
	for _, item := range pool.tickets {
		if item.expiresAt.After(freshest) {
			freshest = item.expiresAt
		}
	}
	if freshest.Sub(now) >= threshold {
		return Plan{Reason: "full", Need: 0, Valid: valid}
	}
	return Plan{Reason: "renew", Need: 1, Replace: true, Valid: valid}
}

// StoreParams 是一次入池所需的参数。
type StoreParams struct {
	// Cap 是本次入池后允许的最大票数；替换模式下传 size+1。
	Cap int
	// TTLSeconds 是第一张票的有效期。
	TTLSeconds int
	// StaggerSeconds 让同批的第 n 张提前 n*stagger 过期，避免整池同时到期。
	StaggerSeconds int
	// MinTTLSeconds 是错开之后的有效期下限。
	MinTTLSeconds int
	// Size 是池的目标容量，入池后按它裁剪。
	Size int
	// TargetLength 是满血票长度，0 表示不按长度判定。
	TargetLength int
}

// StoreRound 把一轮撞票结果写进池：只收满血票，按 state 去重，逐张错开过期时间。
// 返回新增张数。
func (s *Store) StoreRound(accountID int64, model string, records []record, params StoreParams, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensureLocked(poolKey{accountID, model})
	purgeLocked(pool, now, params.TargetLength)

	existing := make(map[string]struct{}, len(pool.tickets))
	for _, item := range pool.tickets {
		existing[item.state] = struct{}{}
	}

	added := 0
	for _, item := range records {
		if len(pool.tickets) >= params.Cap {
			break
		}
		if item.grade != GradeHealthy || item.state == "" {
			continue
		}
		if _, duplicate := existing[item.state]; duplicate {
			continue
		}
		ttl := params.TTLSeconds - added*params.StaggerSeconds
		if ttl < params.MinTTLSeconds {
			ttl = params.MinTTLSeconds
		}
		pool.tickets = append(pool.tickets, ticket{
			state:     item.state,
			length:    item.length,
			createdAt: now,
			expiresAt: now.Add(time.Duration(ttl) * time.Second),
			proxy:     item.proxy,
		})
		existing[item.state] = struct{}{}
		added++
	}
	trimLocked(pool, params.Size)
	return added
}

// NoteRound 记录一轮补池的结果，供状态看板展示。
func (s *Store) NoteRound(accountID int64, model, reason string, added int, grades map[Grade]int, detail string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.ensureLocked(poolKey{accountID, model})
	pool.lastRefillAt = now
	pool.lastReason = reason
	pool.lastAdded = added
	pool.lastError = detail
	if grades != nil {
		pool.lastGrades = grades
	}
}

// Touch 确保某个池存在，让它在票还没撞到时就出现在状态看板里。
func (s *Store) Touch(accountID int64, model string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLocked(poolKey{accountID, model})
}

func (s *Store) ensureLocked(key poolKey) *poolState {
	pool, exists := s.pools[key]
	if !exists {
		pool = &poolState{}
		s.pools[key] = pool
	}
	return pool
}

// purgeLocked 清理过期票与长度不符的票。
//
// 长度判定要在这里再做一次：管理员改了该模型的满血长度之后，
// 旧口径下收进来的票就不该继续被注入。
func purgeLocked(pool *poolState, now time.Time, targetLength int) {
	kept := pool.tickets[:0]
	for _, item := range pool.tickets {
		if !now.Before(item.expiresAt) {
			continue
		}
		if targetLength != 0 && item.length != targetLength {
			continue
		}
		kept = append(kept, item)
	}
	pool.tickets = kept
}

// trimLocked 池超容量时丢最快过期的那几张。
func trimLocked(pool *poolState, size int) {
	if size <= 0 || len(pool.tickets) <= size {
		return
	}
	sort.SliceStable(pool.tickets, func(i, j int) bool {
		return pool.tickets[i].expiresAt.Before(pool.tickets[j].expiresAt)
	})
	pool.tickets = append(pool.tickets[:0:0], pool.tickets[len(pool.tickets)-size:]...)
}

func countValid(pool *poolState, now time.Time) int {
	if pool == nil {
		return 0
	}
	count := 0
	for _, item := range pool.tickets {
		if now.Before(item.expiresAt) {
			count++
		}
	}
	return count
}
