package ticketpool

import (
	"sort"
	"strings"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/proxypool"
)

// TicketStatus 是池里单张有效票的实时状态，配置页点开模型卡片时逐张列出来。
//
// 仍然不含 state 原文——它就是要注入的凭据本身，一律不出账。指纹只够管理员分辨
// 「这几张票是不是同一张」，出口地址也按代理库的规则打过码。
type TicketStatus struct {
	// Fingerprint 是 state 的打码指纹：头 8 尾 6，中间省略。
	Fingerprint string `json:"fingerprint"`
	// Length 是 state 的长度（字符数）。它不参与满血判定，只是个辨识用的数字。
	Length int `json:"length"`
	// AgeSeconds 是这张票撞到多久了。
	AgeSeconds int `json:"age_seconds"`
	// RemainingSeconds 是剩余有效期。
	RemainingSeconds int `json:"remaining_seconds"`
	// Proxy 是撞到它的出口（已打码），空表示直连。
	Proxy string `json:"proxy,omitempty"`
	// InUse 标记它是下一次注入会用的那张（Pick 取最新的一张）。
	InUse bool `json:"in_use,omitempty"`
}

// ModelStatus 是单个 (账号 × 模型) 池的实时状态，直接序列化给配置页看板。
//
// 这里刻意不含任何 state 本身或凭据内容：看板经 config.test 走宿主接口回到浏览器，
// 只允许出现计数、时间与打码后的指纹。
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
	// Tickets 是池里每一张仍然有效的票，按撞到的先后倒序（第一张就是下次要注入的那张）。
	Tickets []TicketStatus `json:"tickets,omitempty"`
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
		// 先按撞到的先后倒序，第一张就是 Pick 会挑的那张（取最新的一张，剩余有效期最长）。
		live := make([]ticket, 0, len(pool.tickets))
		for _, item := range pool.tickets {
			if int(item.expiresAt.Sub(now)/time.Second) <= 0 {
				continue
			}
			live = append(live, item)
		}
		sort.Slice(live, func(i, j int) bool { return live[i].createdAt.After(live[j].createdAt) })
		for index, item := range live {
			remaining := int(item.expiresAt.Sub(now) / time.Second)
			// 空的 proxy 表示这张票是直连撞到的；MaskProxy 会把空串打成 ***，
			// 那样看板就分不出「直连」和「有个出口但没记下来」了。
			outlet := ""
			if item.proxy != "" {
				outlet = proxypool.MaskProxy(item.proxy)
			}
			status.Valid++
			if remaining > status.FreshestSeconds {
				status.FreshestSeconds = remaining
			}
			if status.SoonestSeconds == 0 || remaining < status.SoonestSeconds {
				status.SoonestSeconds = remaining
			}
			status.Tickets = append(status.Tickets, TicketStatus{
				Fingerprint:      fingerprintState(item.state),
				Length:           item.length,
				AgeSeconds:       int(now.Sub(item.createdAt) / time.Second),
				RemainingSeconds: remaining,
				Proxy:            outlet,
				InUse:            index == 0,
			})
		}
		out = append(out, status)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// fingerprintState 把 state 压成能回到浏览器的样子：头 8 尾 6，中间省略。
//
// state 原文就是要注入的凭据，看板一律不带它。短到打不出指纹的（正常不会出现）
// 直接整串打码，免得反倒把全文露出去。
func fingerprintState(state string) string {
	const head, tail = 8, 6
	runes := []rune(state)
	if len(runes) <= head+tail+1 {
		return strings.Repeat("*", len(runes))
	}
	return string(runes[:head]) + "…" + string(runes[len(runes)-tail:])
}
