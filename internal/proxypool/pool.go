// Package proxypool 维护撞票探针要用的代理库：管理员在配置页直接填写
// http/https/socks5/socks5h 代理，插件把启用的条目原样装进内存，供探针轮换出口 IP。
//
// 这里刻意不做连通性测试、也不因失败剔除条目：代理是管理员自己的固定资源，
// 好坏由撞票的真实成败说了算，库只负责随机发牌并累计每条的成功/失败次数。
//
// 整个代理库只存在于内存中：插件进程没有可写目录，配置变更时整体重建。
package proxypool

import (
	"math/rand/v2"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

// entry 是库里的一条代理。url 是完整地址（scheme://[user:pass@]host:port），
// 直接交给 transport 拨号，也是 Note/Lease 的键。
type entry struct {
	url     string
	name    string
	scheme  string
	success int
	fail    int
	lastOK  time.Time
}

// EntryStatus 是单条代理的使用情况，用于状态看板。Addr 已打码。
type EntryStatus struct {
	Name    string    `json:"name"`
	Scheme  string    `json:"scheme"`
	Addr    string    `json:"addr"`
	Success int       `json:"success"`
	Fail    int       `json:"fail"`
	LastOK  time.Time `json:"last_ok"`
}

// Status 是代理库的整体状态快照。
type Status struct {
	Enabled bool          `json:"enabled"`
	Total   int           `json:"total"`
	Entries []EntryStatus `json:"entries"`
}

// Pool 是内存中的代理库，读写并发安全。
type Pool struct {
	enabled bool
	now     func() time.Time

	mu sync.Mutex
	// order 保持配置里的顺序，Status 依它输出；entries 按 URL 索引供 Note 命中。
	order        []string
	entries      map[string]*entry
	randomSource func(int) []int
}

// New 创建代理库。配置必须已经过 pluginconfig 校验（协议、host:port 都已补全）。
func New(config pluginconfig.Proxy, now func() time.Time) *Pool {
	if now == nil {
		now = time.Now
	}
	pool := &Pool{
		enabled: config.Enabled,
		now:     now,
		entries: make(map[string]*entry),
	}
	for _, item := range config.ActiveProxies() {
		if _, exists := pool.entries[item.URL]; exists {
			continue
		}
		pool.entries[item.URL] = &entry{
			url:    item.URL,
			name:   item.Name,
			scheme: item.ProxyScheme(),
		}
		pool.order = append(pool.order, item.URL)
	}
	return pool
}

// Size 返回库里的代理条数。
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// Lease 随机取最多 count 条代理地址，skip 里的地址不会被选中。
//
// 随机而非轮询：撞票靠不同出口 IP 碰运气，固定顺序会让同一批 IP 反复吃到同一个上游节点。
func (p *Pool) Lease(count int, skip map[string]struct{}) []string {
	if count <= 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	candidates := make([]string, 0, len(p.entries))
	for proxyURL := range p.entries {
		if _, skipped := skip[proxyURL]; skipped {
			continue
		}
		candidates = append(candidates, proxyURL)
	}
	// 先排序再洗牌：map 迭代顺序本身是随机的，但排序能让注入的伪随机源在测试中可复现。
	sort.Strings(candidates)
	p.shuffleLocked(candidates)
	if len(candidates) > count {
		candidates = candidates[:count]
	}
	return candidates
}

// Note 记录一次代理使用结果，只累计计数。
//
// 不做自动剔除：代理是管理员填的固定资源，一次撞票失败可能只是上游限流，
// 把它踢出库会让后续每轮都少一个出口，而管理员也无从知道是谁被踢了。
func (p *Pool) Note(proxyURL string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, exists := p.entries[proxyURL]
	if !exists {
		return
	}
	if ok {
		item.success++
		item.lastOK = p.now()
		return
	}
	item.fail++
}

// Status 返回代理库状态快照。
func (p *Pool) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()

	status := Status{
		Enabled: p.enabled,
		Total:   len(p.entries),
		// 空库也输出 []，与 ticketpool.Status.Accounts 保持一致，UI 不用区分 null 与空数组。
		Entries: make([]EntryStatus, 0, len(p.order)),
	}
	for _, proxyURL := range p.order {
		item, exists := p.entries[proxyURL]
		if !exists {
			continue
		}
		status.Entries = append(status.Entries, EntryStatus{
			Name:    item.name,
			Scheme:  item.scheme,
			Addr:    MaskProxy(item.url),
			Success: item.success,
			Fail:    item.fail,
			LastOK:  item.lastOK,
		})
	}
	return status
}

func (p *Pool) shuffleLocked(items []string) {
	if p.randomSource != nil {
		order := p.randomSource(len(items))
		if len(order) == len(items) {
			shuffled := make([]string, len(items))
			for index, position := range order {
				shuffled[index] = items[position]
			}
			copy(items, shuffled)
			return
		}
	}
	rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
}

// MaskProxy 把代理地址压成可以进日志与看板的形式：丢掉用户名密码，
// IPv4 只保留前两段（socks5://1.2.*.*:1080），其它主机名只保留前两个字符。
//
// 代理本身是管理员的付费资源，凭据更是不能外泄，完整打印等于把它写进宿主日志。
func MaskProxy(rawURL string) string {
	if rawURL == "" {
		return "***"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return "***"
	}
	host := parsed.Hostname()
	port := parsed.Port()
	masked := MaskHost(host)
	if port != "" {
		masked = net.JoinHostPort(masked, port)
	}
	if parsed.Scheme == "" {
		return masked
	}
	return parsed.Scheme + "://" + masked
}

// MaskHost 按 MaskProxy 的规则只打码主机部分：IPv4 只保留前两段，其它主机名只保留前两个字符。
func MaskHost(host string) string {
	if host == "" {
		return "***"
	}
	if parts := strings.Split(host, "."); len(parts) == 4 && net.ParseIP(host) != nil {
		return parts[0] + "." + parts[1] + ".*.*"
	}
	if len(host) <= 2 {
		return host + "***"
	}
	return host[:2] + "***"
}
