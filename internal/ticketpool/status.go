package ticketpool

import (
	"sort"
	"time"
)

// ModelStatus 是单个 (账号 × 模型) 池的实时状态，直接序列化给配置页看板。
//
// 这里刻意不含任何 state 本身或凭据内容：看板经 config.test 走宿主接口回到浏览器，
// 只允许出现计数与时间。
type ModelStatus struct {
	Model string `json:"model"`
	Valid int    `json:"valid"`
	Size  int    `json:"size"`
	// FreshestSeconds 是最"新"一张票的剩余有效期（秒）；0 表示池空。
	FreshestSeconds int `json:"freshest_seconds"`
	// SoonestSeconds 是最快过期那张票的剩余有效期（秒）。
	SoonestSeconds int `json:"soonest_seconds"`
	// Reason 是最近一轮补池的判定：empty/partial/cooldown/full/renew。
	Reason string `json:"reason,omitempty"`
	// LastAdded 是最近一轮新增的票数。
	LastAdded int `json:"last_added"`
	// LastRefillSeconds 是距最近一轮补池过去了多少秒；-1 表示还没跑过。
	LastRefillSeconds int `json:"last_refill_seconds"`
	// Grades 是最近一轮各分级的探针数量。
	Grades map[string]int `json:"grades,omitempty"`
	// Detail 是最近一轮的失败摘要。
	Detail string `json:"detail,omitempty"`
	// Hits/Misses 统计转发时取票的命中情况。
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
}

// AccountStatus 是单个账号的票池总览。
type AccountStatus struct {
	AccountID int64  `json:"account_id"`
	Note      string `json:"note,omitempty"`
	// HasCredential 表示已经从过路请求里采到凭据。没有凭据就不会开始撞票。
	HasCredential bool `json:"has_credential"`
	// CredentialSeconds 是凭据的采集时间距今多少秒；-1 表示还没采到。
	CredentialSeconds int           `json:"credential_seconds"`
	Models            []ModelStatus `json:"models"`
}

// Status 是票池管理器的整体状态快照。
type Status struct {
	Running  bool            `json:"running"`
	Accounts []AccountStatus `json:"accounts"`
	// Valid/Wanted 是全部池的有效票数与目标票数，用于一眼看出整体水位。
	Valid  int `json:"valid"`
	Wanted int `json:"wanted"`
}

// ModelStatuses 汇总某账号下所有池的状态。
func (s *Store) ModelStatuses(accountID int64, size int, now time.Time) []ModelStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]ModelStatus, 0, len(s.pools))
	for key, pool := range s.pools {
		if key.account != accountID {
			continue
		}
		status := ModelStatus{
			Model:             key.model,
			Size:              size,
			Reason:            pool.lastReason,
			LastAdded:         pool.lastAdded,
			LastRefillSeconds: -1,
			Detail:            pool.lastError,
			Hits:              pool.hits,
			Misses:            pool.misses,
		}
		if !pool.lastRefillAt.IsZero() {
			status.LastRefillSeconds = int(now.Sub(pool.lastRefillAt) / time.Second)
		}
		if len(pool.lastGrades) > 0 {
			status.Grades = make(map[string]int, len(pool.lastGrades))
			for grade, count := range pool.lastGrades {
				status.Grades[string(grade)] = count
			}
		}
		for _, item := range pool.tickets {
			remaining := int(item.expiresAt.Sub(now) / time.Second)
			if remaining <= 0 {
				continue
			}
			status.Valid++
			if remaining > status.FreshestSeconds {
				status.FreshestSeconds = remaining
			}
			if status.SoonestSeconds == 0 || remaining < status.SoonestSeconds {
				status.SoonestSeconds = remaining
			}
		}
		out = append(out, status)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}
