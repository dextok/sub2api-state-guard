package ticketpool

import (
	"sort"
	"strings"
	"testing"
	"time"
)

const targetLength = 292

func mkRecord(state string, grade Grade) record {
	return record{state: state, length: len(state), grade: grade}
}

func healthyRecord(state string) record {
	padded := state + repeatTo(targetLength-len(state))
	return record{state: padded, length: targetLength, grade: GradeHealthy}
}

// healthyRecordState 铸一张指定长度的满血票，用来验证不同模型各认各的口径。
func healthyRecordState(state string, length int) record {
	padded := state + repeatTo(length-len(state))
	return record{state: padded, length: length, grade: GradeHealthy}
}

func repeatTo(count int) string {
	out := make([]byte, count)
	for index := range out {
		out[index] = 'x'
	}
	return string(out)
}

func storeParams(size int) StoreParams {
	return StoreParams{
		Cap:            size,
		TTLSeconds:     2700,
		StaggerSeconds: 600,
		MinTTLSeconds:  300,
		Size:           size,
		TargetLength:   targetLength,
	}
}

// storeParamsLen 与 storeParams 相同，只是换一个满血长度口径。
func storeParamsLen(size, length int) StoreParams {
	params := storeParams(size)
	params.TargetLength = length
	return params
}

// 决策表来自参考实现，改动它等于改动整个补池节奏，所以逐行钉住。
func TestPlanDecisionTable(t *testing.T) {
	now := time.Unix(1700000000, 0)
	const size, threshold = 3, 600

	t.Run("池空则补满", func(t *testing.T) {
		store := NewStore()
		plan := store.Plan(1, "m", size, threshold, targetLength, now)
		if plan.Reason != "empty" || plan.Need != size || plan.Replace || plan.Valid != 0 {
			t.Fatalf("plan = %+v", plan)
		}
	})

	t.Run("未满但刚补过则冷却", func(t *testing.T) {
		store := NewStore()
		store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(size), now)
		plan := store.Plan(1, "m", size, threshold, targetLength, now.Add(599*time.Second))
		if plan.Reason != "cooldown" || plan.Need != 0 || plan.Valid != 1 {
			t.Fatalf("plan = %+v", plan)
		}
	})

	t.Run("未满且过了冷却则补齐差额", func(t *testing.T) {
		store := NewStore()
		store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(size), now)
		plan := store.Plan(1, "m", size, threshold, targetLength, now.Add(600*time.Second))
		if plan.Reason != "partial" || plan.Need != size-1 || plan.Replace {
			t.Fatalf("plan = %+v", plan)
		}
	})

	t.Run("池满且最新票还够久则不动", func(t *testing.T) {
		store := NewStore()
		store.StoreRound(1, "m", []record{healthyRecord("a"), healthyRecord("b"), healthyRecord("c")},
			storeParams(size), now)
		plan := store.Plan(1, "m", size, threshold, targetLength, now.Add(time.Minute))
		if plan.Reason != "full" || plan.Need != 0 || plan.Valid != size {
			t.Fatalf("plan = %+v", plan)
		}
	})

	t.Run("池满但最新票也快过期则一换一", func(t *testing.T) {
		store := NewStore()
		// 错开幅度调小，让三张票同时还在有效期内、又都快到期。
		params := storeParams(size)
		params.StaggerSeconds = 60
		store.StoreRound(1, "m", []record{healthyRecord("a"), healthyRecord("b"), healthyRecord("c")},
			params, now)
		// 最新一张 2700s 后过期，到 2200s 时只剩 500s < threshold。
		plan := store.Plan(1, "m", size, threshold, targetLength, now.Add(2200*time.Second))
		if plan.Reason != "renew" || plan.Need != 1 || !plan.Replace || plan.Valid != size {
			t.Fatalf("plan = %+v", plan)
		}
	})

	t.Run("过期票不算数", func(t *testing.T) {
		store := NewStore()
		store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(size), now)
		plan := store.Plan(1, "m", size, threshold, targetLength, now.Add(3000*time.Second))
		if plan.Reason != "empty" || plan.Need != size {
			t.Fatalf("plan = %+v", plan)
		}
	})
}

// 管理员改了 target_state_length 之后，旧口径下收进来的票就不该继续被注入。
func TestPlanPurgesTicketsWithStaleLength(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(3), now)
	if valid := store.Valid(1, "m", now); valid != 1 {
		t.Fatalf("入池 %d 张", valid)
	}

	plan := store.Plan(1, "m", 3, 600, 312, now)
	if plan.Reason != "empty" || plan.Valid != 0 {
		t.Fatalf("改判定长度后旧票应被清掉，plan = %+v", plan)
	}
	if _, ok := store.Pick(1, "m", 0, now); ok {
		t.Fatal("旧口径的票不该还能被取到")
	}
}

func TestStoreRoundOnlyKeepsHealthyAndDeduplicates(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	healthy := healthyRecord("a")

	added := store.StoreRound(1, "m", []record{
		healthy,
		healthy, // 同一个 state 重复出现（两个出口撞到同一张）
		mkRecord("mismatched", GradeMismatch),
		mkRecord("weak", GradeWeak),
		{grade: GradeHealthy, state: "", length: 0}, // 满血但没拿到 state
		healthyRecord("b"),
	}, storeParams(5), now)

	if added != 2 {
		t.Fatalf("入池 %d 张，期望 2 张（只收满血且去重）", added)
	}
	if valid := store.Valid(1, "m", now); valid != 2 {
		t.Fatalf("池里 %d 张", valid)
	}
}

// 同批入池的票必须逐张错开过期，否则一到点整池同时失效，会出现无票窗口。
func TestStoreRoundStaggersExpiry(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	params := storeParams(5)

	store.StoreRound(1, "m", []record{
		healthyRecord("a"), healthyRecord("b"), healthyRecord("c"),
	}, params, now)

	statuses := store.ModelStatuses(1, 5, now)
	if len(statuses) != 1 {
		t.Fatalf("池数 = %d", len(statuses))
	}
	status := statuses[0]
	if status.FreshestSeconds != 2700 {
		t.Fatalf("最新一张剩余 %d 秒，期望 2700", status.FreshestSeconds)
	}
	if status.SoonestSeconds != 2700-2*600 {
		t.Fatalf("最快过期那张剩余 %d 秒，期望 %d", status.SoonestSeconds, 2700-2*600)
	}
}

// 错开之后不能把有效期压到地板以下，否则一批票刚入池就没用了。
func TestStoreRoundClampsToMinTTL(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	params := StoreParams{Cap: 5, TTLSeconds: 900, StaggerSeconds: 600,
		MinTTLSeconds: 300, Size: 5, TargetLength: targetLength}

	store.StoreRound(1, "m", []record{
		healthyRecord("a"), healthyRecord("b"), healthyRecord("c"),
	}, params, now)

	status := store.ModelStatuses(1, 5, now)[0]
	if status.SoonestSeconds != 300 {
		t.Fatalf("最快过期那张剩余 %d 秒，期望被 MinTTLSeconds 兜到 300", status.SoonestSeconds)
	}
}

// 一换一：续期时允许临时超存一张，随后丢掉最快过期的那张。
func TestStoreRoundReplacesSoonestExpiring(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	params := storeParams(2)
	// a 在 2700s 后过期，b 因错开提前到 2100s。
	store.StoreRound(1, "m", []record{healthyRecord("a"), healthyRecord("b")}, params, now)

	later := now.Add(1000 * time.Second)
	replace := params
	replace.Cap = 3
	added := store.StoreRound(1, "m", []record{healthyRecord("c")}, replace, later)
	if added != 1 {
		t.Fatalf("续期入池 %d 张", added)
	}
	if valid := store.Valid(1, "m", later); valid != 2 {
		t.Fatalf("续期后池里 %d 张，期望仍是 2 张", valid)
	}
	// 被裁掉的是最快过期的 b，不是刚入池的 c，也不是还能撑 1700s 的 a。
	states := poolStates(t, store, 1, "m")
	if _, present := states["b"]; present {
		t.Fatalf("池里还剩 %v，期望最快过期的 b 被裁掉", sortedKeys(states))
	}
	if _, kept := states["a"]; !kept {
		t.Fatalf("池里还剩 %v，期望 a 保留", sortedKeys(states))
	}
	// 新票最新，转发时应当优先取到它。
	picked, ok := store.Pick(1, "m", 0, later)
	if !ok || !strings.HasPrefix(picked, "c") {
		t.Fatalf("续期后取到 %q…，期望是新票 c", picked[:1])
	}
}

// poolStates 按首字母索引池里的票，便于断言「哪张还在」。
func poolStates(t *testing.T, store *Store, accountID int64, model string) map[string]struct{} {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	pool := store.pools[poolKey{accountID, model}]
	if pool == nil {
		return nil
	}
	out := make(map[string]struct{}, len(pool.tickets))
	for _, item := range pool.tickets {
		out[item.state[:1]] = struct{}{}
	}
	return out
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func TestStoreRoundRespectsCap(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	added := store.StoreRound(1, "m", []record{
		healthyRecord("a"), healthyRecord("b"), healthyRecord("c"),
	}, storeParams(2), now)
	if added != 2 {
		t.Fatalf("入池 %d 张，期望被 Cap 限制在 2 张", added)
	}
}

// 取最新而不是最快过期的：新票剩余有效期最长，能撑过更长的流式请求。
func TestPickPrefersNewestTicket(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(3), now)
	newer := now.Add(time.Minute)
	store.StoreRound(1, "m", []record{healthyRecord("b")}, storeParams(3), newer)

	picked, ok := store.Pick(1, "m", 0, newer)
	if !ok {
		t.Fatal("应当取到票")
	}
	if !strings.HasPrefix(picked, "b") {
		t.Fatalf("取到的是旧票: %q…", picked[:1])
	}
}

func TestPickIsolatesModelsAndAccounts(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "gpt-5-codex", []record{healthyRecord("a")}, storeParams(3), now)

	if _, ok := store.Pick(1, "gpt-5", 0, now); ok {
		t.Fatal("票按模型铸造，绝不能跨模型复用")
	}
	if _, ok := store.Pick(2, "gpt-5-codex", 0, now); ok {
		t.Fatal("票不能跨账号复用")
	}
	if _, ok := store.Pick(1, "", 0, now); ok {
		t.Fatal("模型名为空时不该返回票")
	}
	if _, ok := store.Pick(1, "gpt-5-codex", 0, now); !ok {
		t.Fatal("本模型的票应当取得到")
	}
}

// Ready 是「这个账号值不值得走注入路径」的判断，跨模型看整个账号。
func TestReadyIgnoresModelButRespectsExpiry(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	anyLength := func(string) int { return 0 }
	if store.Ready(1, anyLength, now) {
		t.Fatal("空池不该 Ready")
	}
	store.StoreRound(1, "gpt-5-codex", []record{healthyRecord("a")}, storeParams(3), now)
	if !store.Ready(1, anyLength, now) {
		t.Fatal("有票就该 Ready")
	}
	if store.Ready(2, anyLength, now) {
		t.Fatal("别的账号不该 Ready")
	}
	if store.Ready(1, anyLength, now.Add(2701*time.Second)) {
		t.Fatal("票过期后不该 Ready")
	}
}

// Ready 的口径是按模型取的：同一账号下两个模型各认各的满血长度，
// 只有长度对得上的那个池才让账号 Ready。
func TestReadyResolvesTargetLengthPerModel(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "gpt-5.5", []record{healthyRecordState("a", 292)}, storeParamsLen(3, 292), now)
	store.StoreRound(1, "gpt-6-astra", []record{healthyRecordState("b", 312)}, storeParamsLen(3, 312), now)

	lengths := map[string]int{"gpt-5.5": 292, "gpt-6-astra": 312}
	targetFor := func(model string) int { return lengths[model] }
	if !store.Ready(1, targetFor, now) {
		t.Fatal("两个池的长度都对得上，账号应当 Ready")
	}

	// 把两个池的口径都换成对方的长度，两边就都不合格了。
	swapped := map[string]int{"gpt-5.5": 312, "gpt-6-astra": 292}
	if store.Ready(1, func(model string) int { return swapped[model] }, now) {
		t.Fatal("口径互换后没有任何一个池的票合格，不该 Ready")
	}
	// 只有 astra 的口径错，5.5 那个池仍然让账号 Ready——Ready 是账号级的或。
	half := map[string]int{"gpt-5.5": 292, "gpt-6-astra": 292}
	if !store.Ready(1, func(model string) int { return half[model] }, now) {
		t.Fatal("还有一个池合格时账号应当 Ready")
	}
}

func TestPickCountsHitsAndMisses(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "m", []record{healthyRecord("a")}, storeParams(3), now)

	store.Pick(1, "m", 0, now)
	store.Pick(1, "m", 0, now)
	store.Pick(1, "m", 0, now.Add(3000*time.Second))

	status := store.ModelStatuses(1, 3, now)[0]
	if status.Hits != 2 || status.Misses != 1 {
		t.Fatalf("命中/未命中 = %d/%d，期望 2/1", status.Hits, status.Misses)
	}
}

func TestRetainAndPrune(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.StoreRound(1, "a", []record{healthyRecord("x")}, storeParams(3), now)
	store.StoreRound(1, "b", []record{healthyRecord("y")}, storeParams(3), now)
	store.StoreRound(2, "a", []record{healthyRecord("z")}, storeParams(3), now)

	store.Retain(map[int64]struct{}{1: {}})
	if models := store.Models(2); len(models) != 0 {
		t.Fatalf("账号 2 的池应被清空，实际 %v", models)
	}
	if models := store.Models(1); len(models) != 2 || models[0] != "a" || models[1] != "b" {
		t.Fatalf("账号 1 的池 = %v，期望按名称排序的 [a b]", models)
	}

	// Prune 只收走「不再维护且已经没有有效票」的池：还有票的留到票过期为止。
	store.Prune(1, map[string]struct{}{}, now)
	if models := store.Models(1); len(models) != 2 {
		t.Fatalf("还有有效票的池不该被 Prune 收走，实际 %v", models)
	}
	expired := now.Add(48 * time.Hour)
	store.Prune(1, map[string]struct{}{"b": {}}, expired)
	if models := store.Models(1); len(models) != 1 || models[0] != "b" {
		t.Fatalf("Prune 之后 = %v，期望只剩仍在维护的 b", models)
	}
	// 其它账号的池不受影响。
	store.StoreRound(2, "a", []record{healthyRecord("z")}, storeParams(3), expired)
	store.Prune(1, map[string]struct{}{}, expired.Add(48*time.Hour))
	if models := store.Models(2); len(models) != 1 {
		t.Fatalf("Prune 越界清了别的账号的池：%v", models)
	}
}

// 看板要能显示「配置里写了但还没撞到票」的池，所以 Touch 出来的空池也要出现。
func TestModelStatusesIncludesTouchedEmptyPool(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.Touch(1, "gpt-5-codex")

	statuses := store.ModelStatuses(1, 5, now)
	if len(statuses) != 1 {
		t.Fatalf("池数 = %d", len(statuses))
	}
	status := statuses[0]
	if status.Model != "gpt-5-codex" || status.Valid != 0 || status.Size != 5 {
		t.Fatalf("状态 = %+v", status)
	}
	if status.LastRefillSeconds != -1 {
		t.Fatalf("还没补过池时应为 -1，实际 %d", status.LastRefillSeconds)
	}
}

func TestNoteRoundSurfacesOnDashboard(t *testing.T) {
	now := time.Unix(1700000000, 0)
	store := NewStore()
	store.NoteRound(1, "m", "empty", 0, map[Grade]int{GradeMismatch: 4}, "direct=mismatch(200/312)", now)

	status := store.ModelStatuses(1, 5, now.Add(30*time.Second))[0]
	if status.Reason != "empty" || status.LastAdded != 0 {
		t.Fatalf("状态 = %+v", status)
	}
	if status.LastRefillSeconds != 30 {
		t.Fatalf("距上轮补池 %d 秒，期望 30", status.LastRefillSeconds)
	}
	if status.Grades["mismatch"] != 4 {
		t.Fatalf("分级 = %v", status.Grades)
	}
	if status.Detail != "direct=mismatch(200/312)" {
		t.Fatalf("摘要 = %q", status.Detail)
	}

	// grades 传 nil 表示「这轮没发探针」，不该把上一轮的分级抹掉。
	store.NoteRound(1, "m", "cooldown", 0, nil, "", now)
	if status := store.ModelStatuses(1, 5, now)[0]; status.Grades["mismatch"] != 4 {
		t.Fatalf("分级被 nil 抹掉了: %v", status.Grades)
	}
}
