package transport

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func transportOf(t *testing.T, client *http.Client) *http.Transport {
	t.Helper()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport 类型 = %T，期望 *http.Transport", client.Transport)
	}
	return transport
}

func TestPoolReusesClientPerProxy(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	direct, err := pool.Client("")
	if err != nil {
		t.Fatalf("直连客户端创建失败: %v", err)
	}
	directAgain, err := pool.Client("   ")
	if err != nil {
		t.Fatalf("直连客户端创建失败: %v", err)
	}
	if direct != directAgain {
		t.Fatal("直连应当复用同一个客户端（空白地址等同直连）")
	}

	proxied, err := pool.Client("http://proxy.example.com:8080")
	if err != nil {
		t.Fatalf("代理客户端创建失败: %v", err)
	}
	if proxied == direct {
		t.Fatal("代理与直连必须使用不同的连接池")
	}
	proxiedAgain, err := pool.Client("http://proxy.example.com:8080")
	if err != nil {
		t.Fatalf("代理客户端创建失败: %v", err)
	}
	if proxied != proxiedAgain {
		t.Fatal("同一个代理应当复用同一个客户端")
	}
	if pool.Size() != 2 {
		t.Fatalf("缓存数量 = %d，期望 2", pool.Size())
	}
}

func TestPoolProxyModeDisabledIgnoresHostProxy(t *testing.T) {
	pool := NewPool(mustConfig(t, `{"proxy_mode":"disabled"}`))
	defer pool.Close()

	direct, err := pool.Client("")
	if err != nil {
		t.Fatalf("直连客户端创建失败: %v", err)
	}
	// proxy_mode=disabled 时，宿主下发的代理地址必须被忽略，且不因协议非法而报错。
	ignored, err := pool.Client("socks5://user:password@proxy.example.com:1080")
	if err != nil {
		t.Fatalf("disabled 模式不应校验代理地址: %v", err)
	}
	if ignored != direct {
		t.Fatal("disabled 模式必须复用直连客户端")
	}
	if transportOf(t, ignored).Proxy != nil {
		t.Fatal("disabled 模式不应配置代理")
	}
	if pool.Size() != 1 {
		t.Fatalf("缓存数量 = %d，期望只有直连一项", pool.Size())
	}
}

func TestPoolHTTPProxyIsConfigured(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	client, err := pool.Client("http://proxy.example.com:8080")
	if err != nil {
		t.Fatalf("代理客户端创建失败: %v", err)
	}
	transport := transportOf(t, client)
	if transport.Proxy == nil {
		t.Fatal("http 代理应当走 http.Transport.Proxy")
	}
	request, _ := http.NewRequest(http.MethodGet, "https://chatgpt.com/", nil)
	resolved, err := transport.Proxy(request)
	if err != nil {
		t.Fatalf("解析代理失败: %v", err)
	}
	if resolved == nil || resolved.Host != "proxy.example.com:8080" {
		t.Fatalf("代理地址 = %v", resolved)
	}
}

func TestPoolSOCKS5ProxyReplacesDialer(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	for _, scheme := range []string{"socks5", "socks5h"} {
		client, err := pool.Client(scheme + "://user:password@proxy.example.com:1080")
		if err != nil {
			t.Fatalf("%s 代理客户端创建失败: %v", scheme, err)
		}
		transport := transportOf(t, client)
		if transport.Proxy != nil {
			t.Fatalf("%s 代理不应设置 http.Transport.Proxy", scheme)
		}
		if transport.DialContext == nil {
			t.Fatalf("%s 代理应当替换 DialContext", scheme)
		}
	}
}

func TestPoolRejectsBadProxy(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	cases := []struct {
		name  string
		proxy string
	}{
		{"协议不支持", "ftp://proxy.example.com:1080"},
		{"缺少主机名", "http://"},
		{"无法解析", "socks5://user:password@ex\x7fample.com:1080"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := pool.Client(testCase.proxy)
			if err == nil {
				t.Fatal("非法代理应当报错")
			}
			if strings.Contains(err.Error(), "password") {
				t.Fatalf("错误信息泄露了代理凭据: %v", err)
			}
		})
	}
	if pool.Size() != 0 {
		t.Fatalf("失败的代理不应进入缓存，当前 = %d", pool.Size())
	}
}

func TestPoolEvictsLeastRecentlyUsed(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	first, err := pool.Client("http://proxy-0.example.com:8080")
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	for index := 1; index < maxCachedTransports; index++ {
		if _, err := pool.Client(fmt.Sprintf("http://proxy-%d.example.com:8080", index)); err != nil {
			t.Fatalf("创建失败: %v", err)
		}
	}
	if pool.Size() != maxCachedTransports {
		t.Fatalf("缓存数量 = %d，期望 %d", pool.Size(), maxCachedTransports)
	}

	// 重新使用 proxy-0，它不应该在下一次淘汰中被选中。
	if again, err := pool.Client("http://proxy-0.example.com:8080"); err != nil {
		t.Fatalf("创建失败: %v", err)
	} else if again != first {
		t.Fatal("proxy-0 应当仍在缓存中")
	}

	if _, err := pool.Client("http://proxy-new.example.com:8080"); err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	if pool.Size() != maxCachedTransports {
		t.Fatalf("淘汰后缓存数量 = %d，期望稳定在 %d", pool.Size(), maxCachedTransports)
	}
	if kept, err := pool.Client("http://proxy-0.example.com:8080"); err != nil {
		t.Fatalf("创建失败: %v", err)
	} else if kept != first {
		t.Fatal("最近使用过的 proxy-0 不应被淘汰")
	}
	if pool.Size() != maxCachedTransports {
		t.Fatalf("缓存数量 = %d，期望 %d", pool.Size(), maxCachedTransports)
	}
}

func TestPoolCloseRejectsFurtherUse(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	if _, err := pool.Client(""); err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	pool.Close()
	if pool.Size() != 0 {
		t.Fatalf("关闭后缓存数量 = %d，期望 0", pool.Size())
	}
	if _, err := pool.Client(""); err == nil {
		t.Fatal("关闭后不应再发出新客户端")
	}
	// 重复关闭必须安全：配置切换路径上可能被调用多次。
	pool.Close()
}

func TestPoolTransportMirrorsHostDefaults(t *testing.T) {
	config := mustConfig(t, "")
	pool := NewPool(config)
	defer pool.Close()

	client, err := pool.Client("")
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	if client.Timeout != 0 {
		t.Fatalf("http.Client.Timeout = %v，必须为 0，否则流式响应会被掐断", client.Timeout)
	}
	if client.CheckRedirect == nil {
		t.Fatal("必须禁止自动跟随重定向")
	}

	transport := transportOf(t, client)
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"TLSHandshakeTimeout", transport.TLSHandshakeTimeout, 10 * time.Second},
		{"ResponseHeaderTimeout", transport.ResponseHeaderTimeout, 300 * time.Second},
		{"IdleConnTimeout", transport.IdleConnTimeout, 90 * time.Second},
		{"MaxIdleConns", transport.MaxIdleConns, 240},
		{"MaxIdleConnsPerHost", transport.MaxIdleConnsPerHost, 120},
		{"MaxConnsPerHost", transport.MaxConnsPerHost, 240},
		{"ExpectContinueTimeout", transport.ExpectContinueTimeout, time.Second},
		{"ForceAttemptHTTP2", transport.ForceAttemptHTTP2, true},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v，期望 %v（需与宿主 http_upstream.go 对齐）", check.name, check.got, check.want)
		}
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS 最低版本 = %v，期望 TLS 1.2", transport.TLSClientConfig)
	}
	if !http2Enabled(transport) {
		t.Fatal("启用 HTTP/2 时应当协商 h2（带 PING 探测）")
	}
}

// http2Enabled 兼容两种 x/net/http2 接线方式：Go 1.27 之前写 TLSNextProto["h2"]，
// Go 1.27 起改为设置 http.Transport.Protocols。
func http2Enabled(transport *http.Transport) bool {
	if transport.TLSNextProto["h2"] != nil {
		return true
	}
	return transport.Protocols != nil && transport.Protocols.HTTP2()
}

func TestPoolHTTP2Disabled(t *testing.T) {
	pool := NewPool(mustConfig(t, `{"enable_http2":false}`))
	defer pool.Close()

	client, err := pool.Client("")
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	transport := transportOf(t, client)
	if transport.ForceAttemptHTTP2 {
		t.Fatal("enable_http2=false 时不应尝试 HTTP/2")
	}
	if transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 {
		t.Fatalf("enable_http2=false 时应当用空表显式关闭协商，得到 %v", transport.TLSNextProto)
	}
	if http2Enabled(transport) {
		t.Fatal("enable_http2=false 时 net/http 不应协商 HTTP/2")
	}
}

func TestPoolTLSMinVersion13(t *testing.T) {
	pool := NewPool(mustConfig(t, `{"tls_min_version":"1.3"}`))
	defer pool.Close()

	client, err := pool.Client("")
	if err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	if got := transportOf(t, client).TLSClientConfig.MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("TLS 最低版本 = %d，期望 TLS 1.3", got)
	}
}

func TestPoolConcurrentClients(t *testing.T) {
	pool := NewPool(mustConfig(t, ""))
	defer pool.Close()

	const workers = 16
	done := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer func() { done <- struct{}{} }()
			for round := 0; round < 20; round++ {
				proxyURL := fmt.Sprintf("http://proxy-%d.example.com:8080", (worker+round)%4)
				if _, err := pool.Client(proxyURL); err != nil {
					t.Errorf("并发创建失败: %v", err)
					return
				}
				if _, err := pool.Client(""); err != nil {
					t.Errorf("并发创建失败: %v", err)
					return
				}
			}
		}(worker)
	}
	for worker := 0; worker < workers; worker++ {
		<-done
	}
	if pool.Size() != 5 {
		t.Fatalf("缓存数量 = %d，期望 4 个代理 + 1 个直连", pool.Size())
	}
}
