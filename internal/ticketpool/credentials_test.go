package ticketpool

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHarvestCredential(t *testing.T) {
	now := time.Unix(1700000000, 0)
	header := http.Header{}
	// 键名大小写由宿主决定，解析必须忽略大小写。
	header.Set("authorization", "Bearer "+strings.Repeat("a", 40))
	header.Set("chatgpt-account-id", "acct-9")
	header.Set("user-agent", "codex_cli_rs/0.154.0")

	cred, ok := harvestCredential(header, "https://chatgpt.com/backend-api/codex/responses", now)
	if !ok {
		t.Fatal("应当采到凭据")
	}
	if cred.Authorization != "Bearer "+strings.Repeat("a", 40) {
		t.Fatalf("Authorization = %q", cred.Authorization)
	}
	if cred.AccountHeader != "acct-9" || cred.UserAgent != "codex_cli_rs/0.154.0" {
		t.Fatalf("凭据 = %+v", cred)
	}
	if cred.GatewayBase != "https://chatgpt.com/backend-api/codex" {
		t.Fatalf("网关根 = %q", cred.GatewayBase)
	}
	if !cred.UpdatedAt.Equal(now) {
		t.Fatalf("采集时间 = %v", cred.UpdatedAt)
	}
}

func TestHarvestCredentialRejectsUnusableAuthorization(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cases := map[string]string{
		"空":         "",
		"不是 Bearer": "Basic " + strings.Repeat("a", 40),
		"只有前缀":      "Bearer ",
		"令牌太短":      "Bearer short",
		"带控制字符":     "Bearer " + strings.Repeat("a", 40) + "\r\nX-Injected: 1",
		"超长":        "Bearer " + strings.Repeat("a", maxAuthorizationBytes),
	}
	for name, value := range cases {
		header := http.Header{}
		if value != "" {
			header.Set("Authorization", value)
		}
		if _, ok := harvestCredential(header, "https://host/backend-api/codex/responses", now); ok {
			t.Errorf("%s：不该采到凭据", name)
		}
	}
}

// 账号头与 UA 会被原样塞回探针请求头，带控制字符就是头注入。
func TestHarvestCredentialDropsUnsafeSideHeaders(t *testing.T) {
	now := time.Unix(1700000000, 0)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+strings.Repeat("a", 40))
	// http.Header.Set 会拒绝带换行的值，所以直接构造底层切片。
	header["Chatgpt-Account-Id"] = []string{"acct\r\nX-Injected: 1"}
	header["User-Agent"] = []string{strings.Repeat("u", maxUserAgentBytes+1)}

	cred, ok := harvestCredential(header, "", now)
	if !ok {
		t.Fatal("应当采到凭据")
	}
	if cred.AccountHeader != "" {
		t.Fatalf("带控制字符的账号头应被丢弃，实际 %q", cred.AccountHeader)
	}
	if cred.UserAgent != "" {
		t.Fatalf("超长 UA 应被丢弃，实际 %q", cred.UserAgent)
	}
}

func TestGatewayBaseFromURL(t *testing.T) {
	cases := map[string]string{
		"https://chatgpt.com/backend-api/codex/responses":  "https://chatgpt.com/backend-api/codex",
		"https://chatgpt.com/backend-api/codex/responses/": "https://chatgpt.com/backend-api/codex",
		"http://relay.internal:8080/v1/responses":          "http://relay.internal:8080/v1",
		"https://host/responses?stream=true":               "https://host",
		// 不是 /responses 端点 → 回落到配置值。
		"https://host/backend-api/codex/models": "",
		"https://host/":                         "",
		// 协议与主机缺失的一律拒绝，免得拼出个奇怪的探针地址。
		"ftp://host/responses":         "",
		"/backend-api/codex/responses": "",
		"":                             "",
		"://bad":                       "",
	}
	for rawURL, want := range cases {
		if got := gatewayBaseFromURL(rawURL); got != want {
			t.Errorf("gatewayBaseFromURL(%q) = %q，期望 %q", rawURL, got, want)
		}
	}
}

// Observe 在转发热路径上，同一份凭据不该每次都拿写锁；
// 但令牌一换（刷新过了）必须立刻生效，否则撞票会一直用过期令牌。
func TestCredentialStoreObserveRefreshPolicy(t *testing.T) {
	store := NewCredentialStore()
	base := time.Unix(1700000000, 0)
	cred := Credential{Authorization: "Bearer aaa", GatewayBase: "https://host/v1", UpdatedAt: base}

	if !store.Observe(1, cred) {
		t.Fatal("首次采集应当返回 true")
	}
	same := cred
	same.UpdatedAt = base.Add(time.Second)
	if store.Observe(1, same) {
		t.Fatal("同一份凭据在刷新间隔内不该写入")
	}
	if got, _ := store.Get(1); !got.UpdatedAt.Equal(base) {
		t.Fatalf("时间戳不该被刷新，实际 %v", got.UpdatedAt)
	}

	stale := cred
	stale.UpdatedAt = base.Add(credentialRefreshInterval + time.Second)
	if store.Observe(1, stale) {
		t.Fatal("只是刷新时间戳，不算换了凭据")
	}
	if got, _ := store.Get(1); !got.UpdatedAt.Equal(stale.UpdatedAt) {
		t.Fatalf("超过刷新间隔后应当更新时间戳，实际 %v", got.UpdatedAt)
	}

	rotated := cred
	rotated.Authorization = "Bearer bbb"
	rotated.UpdatedAt = base.Add(time.Second)
	if !store.Observe(1, rotated) {
		t.Fatal("令牌变化应当立刻写入并返回 true")
	}
	if got, _ := store.Get(1); got.Authorization != "Bearer bbb" {
		t.Fatalf("凭据未更新: %q", got.Authorization)
	}
}

// 管理员一取消接管，账号的 access token 就不该继续留在插件内存里。
func TestCredentialStoreRetainWipesUnmanagedAccounts(t *testing.T) {
	store := NewCredentialStore()
	store.Put(1, Credential{Authorization: "Bearer aaa"})
	store.Put(2, Credential{Authorization: "Bearer bbb"})

	store.Retain(map[int64]struct{}{1: {}})

	if !store.Has(1) {
		t.Fatal("仍被接管的账号凭据不该被抹掉")
	}
	if store.Has(2) {
		t.Fatal("不再被接管的账号凭据必须被抹掉")
	}
	if size := store.Size(); size != 1 {
		t.Fatalf("凭据数 = %d", size)
	}
}
