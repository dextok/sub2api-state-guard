package proxypool

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

// testConfig 造一份已经规范化过的代理库配置：Pool 假定 pluginconfig 已经补好
// 协议与端口，这里直接写全，免得用例依赖校验层的实现细节。
func testConfig(items ...pluginconfig.ProxyItem) pluginconfig.Proxy {
	return pluginconfig.Proxy{Enabled: true, Proxies: items}
}

func item(name, url string) pluginconfig.ProxyItem {
	return pluginconfig.ProxyItem{Name: name, URL: url, Enabled: true}
}

// newTestPool 造一个时间与洗牌都可控的代理库，Lease 的结果才可复现。
func newTestPool(config pluginconfig.Proxy, now *time.Time) *Pool {
	pool := New(config, func() time.Time { return *now })
	pool.randomSource = func(size int) []int {
		order := make([]int, size)
		for index := range order {
			order[index] = index
		}
		return order
	}
	return pool
}

func TestNewLoadsEnabledItemsOnly(t *testing.T) {
	now := time.Unix(1700000000, 0)
	config := testConfig(
		item("hk", "socks5://1.1.1.1:1080"),
		pluginconfig.ProxyItem{Name: "off", URL: "http://2.2.2.2:8080", Enabled: false},
		item("dup", "socks5://1.1.1.1:1080"), // 与第一条同址，只该留一条
		item("us", "http://3.3.3.3:8080"),
	)
	pool := newTestPool(config, &now)

	if size := pool.Size(); size != 2 {
		t.Fatalf("库里 %d 条，期望 2 条（停用的不入库、重复的只留一条）", size)
	}
	status := pool.Status()
	if !status.Enabled || status.Total != 2 || len(status.Entries) != 2 {
		t.Fatalf("状态 = %+v", status)
	}
	// Status 按配置顺序输出，管理员才能把看板行和配置表格对上。
	if status.Entries[0].Name != "hk" || status.Entries[1].Name != "us" {
		t.Fatalf("条目顺序 = %+v，期望跟随配置顺序", status.Entries)
	}
	if status.Entries[0].Scheme != pluginconfig.ProxySchemeSOCKS5 || status.Entries[1].Scheme != pluginconfig.ProxySchemeHTTP {
		t.Fatalf("协议未按条目区分: %+v", status.Entries)
	}

	// 代理库总开关关掉后一条都不装，状态也如实回报。
	config.Enabled = false
	off := newTestPool(config, &now)
	if size := off.Size(); size != 0 {
		t.Fatalf("代理库关闭时库里 %d 条", size)
	}
	if status := off.Status(); status.Enabled || status.Total != 0 || len(status.Entries) != 0 {
		t.Fatalf("关闭时状态 = %+v", status)
	}
}

func TestLeaseSkipsAndLimits(t *testing.T) {
	now := time.Unix(1700000000, 0)
	pool := newTestPool(testConfig(
		item("a", "socks5://1.1.1.1:1080"),
		item("b", "socks5://2.2.2.2:1080"),
		item("c", "http://3.3.3.3:8080"),
	), &now)

	if got := pool.Lease(0, nil); got != nil {
		t.Fatalf("要 0 条应当返回空，实际 %v", got)
	}
	if got := pool.Lease(2, nil); len(got) != 2 {
		t.Fatalf("要 2 条拿到 %v", got)
	}
	if got := pool.Lease(10, nil); len(got) != 3 {
		t.Fatalf("库里只有 3 条，却拿到 %v", got)
	}
	// Lease 返回的是完整 URL，探针要照它的协议拨号。
	for _, leased := range pool.Lease(10, nil) {
		if !strings.Contains(leased, "://") {
			t.Fatalf("Lease 返回了不带协议的地址: %q", leased)
		}
	}
	skip := map[string]struct{}{"socks5://1.1.1.1:1080": {}, "socks5://2.2.2.2:1080": {}}
	got := pool.Lease(10, skip)
	if len(got) != 1 || got[0] != "http://3.3.3.3:8080" {
		t.Fatalf("换代理重试时不能再拿已用过的出口，实际 %v", got)
	}
}

// 撞票靠不同出口碰运气：Lease 必须是随机发牌，固定顺序会让同一批 IP
// 每轮都撞在同一个上游节点上。
func TestLeaseShufflesAcrossCalls(t *testing.T) {
	now := time.Unix(1700000000, 0)
	items := make([]pluginconfig.ProxyItem, 0, 8)
	for index := 1; index <= 8; index++ {
		items = append(items, pluginconfig.ProxyItem{
			URL:     "socks5://10.0.0." + string(rune('0'+index)) + ":1080",
			Enabled: true,
		})
	}
	// 这里用真随机（不注入 randomSource），验证默认行为确实会洗牌。
	pool := New(testConfig(items...), func() time.Time { return now })

	first := pool.Lease(8, nil)
	if len(first) != 8 {
		t.Fatalf("拿到 %d 条", len(first))
	}
	differs := false
	for attempt := 0; attempt < 20 && !differs; attempt++ {
		next := pool.Lease(8, nil)
		for index := range next {
			if next[index] != first[index] {
				differs = true
				break
			}
		}
	}
	if !differs {
		t.Fatal("连续 20 次 Lease 顺序完全一致，说明没有洗牌")
	}

	// 洗牌不能丢条目：每次都必须是同一个集合。
	got := append([]string(nil), pool.Lease(8, nil)...)
	sort.Strings(got)
	want := append([]string(nil), first...)
	sort.Strings(want)
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("洗牌后集合变了: %v vs %v", got, want)
		}
	}
}

// 用户选择「照单全收」：失败只记数，不剔除。剔除会让管理员填进去的出口
// 悄悄消失，而看板上又看不出是谁被踢了。
func TestNoteCountsWithoutEviction(t *testing.T) {
	now := time.Unix(1700000000, 0)
	const addr = "socks5://1.1.1.1:1080"
	pool := newTestPool(testConfig(item("hk", addr)), &now)

	for index := 0; index < 5; index++ {
		pool.Note(addr, false)
	}
	if size := pool.Size(); size != 1 {
		t.Fatalf("连续失败后库里 %d 条，期望仍是 1 条", size)
	}
	if got := pool.Lease(1, nil); len(got) != 1 {
		t.Fatalf("失败过的代理仍应可被派发，实际 %v", got)
	}

	now = now.Add(time.Minute)
	pool.Note(addr, true)
	pool.Note(addr, true)

	entry := pool.Status().Entries[0]
	if entry.Success != 2 || entry.Fail != 5 {
		t.Fatalf("计数 = 成功 %d / 失败 %d，期望 2 / 5", entry.Success, entry.Fail)
	}
	if !entry.LastOK.Equal(now) {
		t.Fatalf("last_ok = %v，期望最后一次成功的时刻 %v", entry.LastOK, now)
	}

	// 对不在库里的地址反馈不能 panic，也不能凭空建条目。
	pool.Note("socks5://9.9.9.9:1080", false)
	pool.Note("", true)
	if size := pool.Size(); size != 1 {
		t.Fatalf("反馈未知地址后库里 %d 条", size)
	}
}

// 状态看板经宿主接口回到浏览器，配置页也会被截图：地址必须打码，
// 内联的用户名密码更是一个字都不能出现。
func TestStatusMasksAddressesAndDropsCredentials(t *testing.T) {
	now := time.Unix(1700000000, 0)
	pool := newTestPool(testConfig(
		item("hk", "socks5://user:s3cr3t@1.2.3.4:1080"),
		item("named", "http://proxy.example.com:8080"),
	), &now)

	for _, entry := range pool.Status().Entries {
		if strings.Contains(entry.Addr, "s3cr3t") || strings.Contains(entry.Addr, "user") {
			t.Fatalf("状态里带出了代理凭据: %q", entry.Addr)
		}
		if strings.Contains(entry.Addr, "1.2.3.4") || strings.Contains(entry.Addr, "proxy.example.com") {
			t.Fatalf("状态里带出了完整地址: %q", entry.Addr)
		}
	}
	if got := pool.Status().Entries[0].Addr; got != "socks5://1.2.*.*:1080" {
		t.Fatalf("打码结果 = %q", got)
	}
	if got := pool.Status().Entries[1].Addr; got != "http://pr***:8080" {
		t.Fatalf("打码结果 = %q", got)
	}
}

func TestMaskProxy(t *testing.T) {
	cases := map[string]string{
		"socks5://1.2.3.4:1080":         "socks5://1.2.*.*:1080",
		"socks5h://10.0.0.1:9050":       "socks5h://10.0.*.*:9050",
		"http://u:p@203.0.113.9:8080":   "http://203.0.*.*:8080",
		"https://proxy.example.com:443": "https://pr***:443",
		"socks5://[2001:db8::1]:1080":   "socks5://20***:1080",
		"socks5://a.b:1080":             "socks5://a.***:1080",
		"1.2.3.4:1080":                  "***",
		"":                              "***",
		"://nope":                       "***",
	}
	for raw, want := range cases {
		if got := MaskProxy(raw); got != want {
			t.Errorf("MaskProxy(%q) = %q，期望 %q", raw, got, want)
		}
	}
}
