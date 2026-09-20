// Package ticketpool 在插件进程内自建算力票（x-codex-turn-state）池：
// 用从过路请求里采到的账号凭据，经代理库（http/https/socks5/socks5h）轮换出口向 codex 网关
// 发探针「撞票」，只把满血票（200 且上游自报的模型与请求的一致）按 (账号 × 模型) 收进池子，转发时就地注入。
//
// 算法对齐参考实现 codex-ticket-pool（backend/app/capture.py），见 capture.go 与 store.go。
// 插件没有可写目录，票池与凭据都只存在于内存里，进程重启后重新积累。
package ticketpool

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// 凭据字段的长度上限。Authorization 是 JWT，实测千余字节，这里留足余量。
const (
	maxAuthorizationBytes = 8192
	maxAccountHeaderBytes = 200
	maxUserAgentBytes     = 200
	// credentialRefreshInterval 是「同一份凭据」刷新时间戳的最小间隔。
	// Harvest 在每个请求的热路径上，没必要为了一个时间戳每次都拿写锁。
	credentialRefreshInterval = time.Minute
)

// Credential 是撞票所需的账号凭据，全部来自过路请求。
//
// 它只存在于内存中：不落盘、不进日志、不出现在健康检查与配置测试的输出里。
// 状态看板只会显示「有/无凭据」与采集时间。
type Credential struct {
	// Authorization 是完整的头值，形如 "Bearer eyJ..."。
	Authorization string
	// AccountHeader 是 ChatGpt-Account-Id 头值，可能为空。
	AccountHeader string
	// UserAgent 沿用真实客户端的 UA，空则回落到配置值。
	UserAgent string
	// GatewayBase 是从请求 URL 推出的网关根（去掉末尾的 /responses），
	// 空则回落到配置的 gateway_base_url。
	GatewayBase string
	UpdatedAt   time.Time
}

// CredentialStore 是账号凭据的内存表。
type CredentialStore struct {
	mu    sync.RWMutex
	items map[int64]Credential
}

// NewCredentialStore 创建空的凭据表。
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{items: make(map[int64]Credential)}
}

// Put 写入或刷新账号凭据。
func (s *CredentialStore) Put(accountID int64, cred Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[accountID] = cred
}

// Get 读取账号凭据。
func (s *CredentialStore) Get(accountID int64) (Credential, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cred, ok := s.items[accountID]
	return cred, ok
}

// Has 判断账号是否已经采到过凭据。
func (s *CredentialStore) Has(accountID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.items[accountID]
	return ok
}

// Size 返回已有凭据的账号数。
func (s *CredentialStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Retain 只保留仍被接管的账号凭据，其余立即抹掉。
// 管理员一关掉某个账号，它的 token 就不该继续留在插件内存里。
func (s *CredentialStore) Retain(active map[int64]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for accountID := range s.items {
		if _, keep := active[accountID]; !keep {
			delete(s.items, accountID)
		}
	}
}

// Observe 从一次过路请求里采集凭据，返回是否写入了新值。
//
// 同一份凭据重复出现时只在超过 credentialRefreshInterval 后刷新时间戳，
// 避免在转发热路径上反复拿写锁。
func (s *CredentialStore) Observe(accountID int64, cred Credential) bool {
	s.mu.RLock()
	existing, exists := s.items[accountID]
	s.mu.RUnlock()
	if exists && existing.Authorization == cred.Authorization &&
		existing.AccountHeader == cred.AccountHeader &&
		existing.GatewayBase == cred.GatewayBase &&
		cred.UpdatedAt.Sub(existing.UpdatedAt) < credentialRefreshInterval {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[accountID] = cred
	return !exists || existing.Authorization != cred.Authorization
}

// harvestCredential 从请求头与 URL 中解析出撞票凭据。
// 取不到可用的 Authorization 时返回 false——没有它撞票根本发不出去。
func harvestCredential(header http.Header, rawURL string, now time.Time) (Credential, bool) {
	authorization := strings.TrimSpace(headerLookup(header, "Authorization"))
	if !isUsableBearer(authorization) {
		return Credential{}, false
	}
	cred := Credential{
		Authorization: authorization,
		AccountHeader: safeHeaderValue(headerLookup(header, "ChatGpt-Account-Id"), maxAccountHeaderBytes),
		UserAgent:     safeHeaderValue(headerLookup(header, "User-Agent"), maxUserAgentBytes),
		GatewayBase:   gatewayBaseFromURL(rawURL),
		UpdatedAt:     now,
	}
	return cred, true
}

// isUsableBearer 只接受 Bearer 令牌，并挡掉明显不是令牌的值。
func isUsableBearer(value string) bool {
	if len(value) > maxAuthorizationBytes {
		return false
	}
	const prefix = "bearer "
	if len(value) <= len(prefix) || !strings.EqualFold(value[:len(prefix)], prefix) {
		return false
	}
	token := strings.TrimSpace(value[len(prefix):])
	// 太短的不可能是 OAuth access token，多半是占位或测试值。
	if len(token) < 20 {
		return false
	}
	return !containsControlCharacter(value)
}

// safeHeaderValue 过滤掉不能安全塞回请求头的值。
func safeHeaderValue(value string, limit int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > limit || containsControlCharacter(value) {
		return ""
	}
	return value
}

// gatewayBaseFromURL 从真实请求地址推出网关根：
// https://host/backend-api/codex/responses → https://host/backend-api/codex
//
// 这样插件跟着宿主实际在用的端点走，换中转、换域名都不需要改插件配置；
// 推不出来（不是 /responses 端点）就回落到配置值。
func gatewayBaseFromURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	path := strings.TrimSuffix(parsed.Path, "/")
	const endpoint = "/responses"
	if !strings.HasSuffix(path, endpoint) {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host + strings.TrimSuffix(path, endpoint)
}

// headerLookup 忽略大小写查头值：帧里的键名大小写由宿主决定，不能假定已规范化。
func headerLookup(header http.Header, name string) string {
	for key, values := range header {
		if !strings.EqualFold(key, name) || len(values) == 0 {
			continue
		}
		return values[0]
	}
	return ""
}

func containsControlCharacter(value string) bool {
	for index := 0; index < len(value); index++ {
		char := value[index]
		if char == '\t' {
			continue
		}
		if char < 0x20 || char == 0x7f {
			return true
		}
	}
	return false
}
