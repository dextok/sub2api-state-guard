// Package transport 实现插件接管出站请求后需要的完整 HTTP 传输能力：
// 按代理缓存的连接池、与宿主对齐的连接参数，以及过载防护头的注入策略。
//
// 插件一旦命中某个账号，就取代了宿主的 HTTPUpstream，因此这里的默认值必须对齐
// backend/internal/repository/http_upstream.go，避免连接行为出现落差。
package transport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"

	"github.com/dextok/sub2api-state-guard/internal/pluginconfig"
)

const (
	// maxCachedTransports 限制按代理缓存的 Transport 数量，超出按 LRU 淘汰。
	maxCachedTransports = 64
	// directKey 是直连的缓存键。
	directKey = "direct"
	// dialKeepAlive 与 Go 默认值一致。
	dialKeepAlive = 30 * time.Second
	// http2PingTimeout 对齐宿主长流探测参数。
	http2PingTimeout = 5 * time.Second
)

// Pool 按代理地址缓存 *http.Transport 与其上的 *http.Client。
// 同一份配置下的所有请求共享连接池；配置切换时由调用方 Close 旧池。
type Pool struct {
	config pluginconfig.Config

	mu      sync.Mutex
	entries map[string]*http.Client
	order   []string // 最近使用的排在末尾
	closed  bool
}

// NewPool 用给定配置创建连接池。配置必须已经过 pluginconfig 校验。
func NewPool(config pluginconfig.Config) *Pool {
	return &Pool{
		config:  config,
		entries: make(map[string]*http.Client),
	}
}

// Client 返回对应代理的 HTTP 客户端。proxyURL 为空表示直连。
//
// 客户端本身不设 Timeout：整体超时由调用方通过 context 控制，
// 否则流式响应会被 http.Client.Timeout 从中间掐断。
func (p *Pool) Client(proxyURL string) (*http.Client, error) {
	target, key, err := p.resolveProxy(proxyURL)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("连接池已关闭")
	}
	if client, ok := p.entries[key]; ok {
		p.touchLocked(key)
		return client, nil
	}

	transport, err := p.buildTransport(target)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: transport,
		// 出站请求的重定向由上游语义决定，插件不擅自跟随。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	p.entries[key] = client
	p.order = append(p.order, key)
	p.evictLocked()
	return client, nil
}

// Close 关闭所有缓存连接的空闲连接。配置切换时调用，正在进行的请求不受影响。
func (p *Pool) Close() {
	p.mu.Lock()
	clients := make([]*http.Client, 0, len(p.entries))
	for _, client := range p.entries {
		clients = append(clients, client)
	}
	p.entries = make(map[string]*http.Client)
	p.order = nil
	p.closed = true
	p.mu.Unlock()

	for _, client := range clients {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

// Size 返回当前缓存的客户端数量，仅用于测试与诊断。
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// resolveProxy 解析代理地址并给出缓存键。proxy_mode=disabled 时忽略宿主下发的代理。
func (p *Pool) resolveProxy(proxyURL string) (*url.URL, string, error) {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" || p.config.ProxyMode == pluginconfig.ProxyModeDisabled {
		return nil, directKey, nil
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		// 刻意不带上原始地址：代理地址里可能有用户名密码。
		return nil, "", errors.New("代理地址无法解析")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, "", fmt.Errorf("不支持的代理协议 %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, "", errors.New("代理地址缺少主机名")
	}
	// 缓存键用完整地址（含凭据）区分不同代理身份，但它只留在进程内存里，
	// 不会出现在日志、诊断或错误信息中。
	return parsed, parsed.String(), nil
}

func (p *Pool) touchLocked(key string) {
	for index, existing := range p.order {
		if existing != key {
			continue
		}
		p.order = append(p.order[:index], p.order[index+1:]...)
		break
	}
	p.order = append(p.order, key)
}

func (p *Pool) evictLocked() {
	for len(p.order) > maxCachedTransports {
		oldest := p.order[0]
		p.order = p.order[1:]
		client, ok := p.entries[oldest]
		if !ok {
			continue
		}
		delete(p.entries, oldest)
		if transport, transportOK := client.Transport.(*http.Transport); transportOK {
			// 正在进行的请求仍持有该 Transport，这里只回收空闲连接。
			transport.CloseIdleConnections()
		}
	}
}

func (p *Pool) buildTransport(proxyURL *url.URL) (*http.Transport, error) {
	config := p.config
	dialer := &net.Dialer{
		Timeout:   seconds(config.DialTimeoutSeconds),
		KeepAlive: dialKeepAlive,
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   seconds(config.TLSHandshakeTimeoutSeconds),
		MaxIdleConns:          config.MaxIdleConnections,
		MaxIdleConnsPerHost:   config.MaxIdleConnectionsPerHost,
		MaxConnsPerHost:       config.MaxConnectionsPerHost,
		IdleConnTimeout:       seconds(config.IdleConnectionTimeoutSeconds),
		ResponseHeaderTimeout: seconds(config.ResponseHeaderTimeoutSeconds),
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     config.EnableHTTP2,
		TLSClientConfig:       &tls.Config{MinVersion: tlsMinVersion(config.TLSMinVersion)},
	}

	if proxyURL != nil {
		if err := configureProxy(transport, proxyURL, dialer); err != nil {
			return nil, err
		}
	}

	if !config.EnableHTTP2 {
		// 显式关闭 HTTP/2 协商，保证真的走 HTTP/1.1。
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
		return transport, nil
	}
	// 池化连接被代理/NAT 静默掐断会成为死连接，长流请求会挂到 TCP 重传超时。
	// 与宿主一致：启用 HTTP/2 主动 PING 探测。
	http2Transport, err := http2.ConfigureTransports(transport)
	if err != nil {
		return nil, fmt.Errorf("配置 HTTP/2 失败: %w", err)
	}
	if http2Transport != nil && config.HTTP2ReadIdleTimeoutSeconds > 0 {
		http2Transport.ReadIdleTimeout = seconds(config.HTTP2ReadIdleTimeoutSeconds)
		http2Transport.PingTimeout = http2PingTimeout
	}
	return transport, nil
}

func configureProxy(transport *http.Transport, proxyURL *url.URL, dialer *net.Dialer) error {
	switch strings.ToLower(proxyURL.Scheme) {
	case "http", "https":
		transport.Proxy = http.ProxyURL(proxyURL)
		return nil
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if proxyURL.User != nil {
			password, _ := proxyURL.User.Password()
			auth = &proxy.Auth{User: proxyURL.User.Username(), Password: password}
		}
		socksDialer, err := proxy.SOCKS5("tcp", proxyURL.Host, auth, dialer)
		if err != nil {
			return fmt.Errorf("创建 SOCKS5 代理连接失败: %w", err)
		}
		contextDialer, ok := socksDialer.(proxy.ContextDialer)
		if !ok {
			return errors.New("SOCKS5 代理不支持 context 取消")
		}
		transport.DialContext = contextDialer.DialContext
		return nil
	default:
		return fmt.Errorf("不支持的代理协议 %q", proxyURL.Scheme)
	}
}

func tlsMinVersion(value string) uint16 {
	if value == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

func seconds(value int) time.Duration {
	if value <= 0 {
		return 0
	}
	return time.Duration(value) * time.Second
}
