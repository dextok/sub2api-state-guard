package ticketpool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/pluginconfig"
	"github.com/dextok/sub2api-plugin-overload-guard/internal/proxypool"
)

// Grade 是一次探针结果的分级，取值与判定标准对齐参考实现：
//
//	healthy    满血：200 + 有产出 + 长度等于 target_state_length —— 唯一入池
//	mismatch   长度不符（如 312 = premium 池，已验收确认降智），不入池
//	overloaded 过载：SSE error 里带 overload，上游明确表示这个节点现在不接活
//	weak       非 200 但带回了 state 头
//	blocked    非 200 且没有 state 头
//	partial    200 但既无产出也未完成
//	error      连接异常等，连响应都没拿到
type Grade string

const (
	GradeHealthy    Grade = "healthy"
	GradeMismatch   Grade = "mismatch"
	GradeOverloaded Grade = "overloaded"
	GradeWeak       Grade = "weak"
	GradeBlocked    Grade = "blocked"
	GradePartial    Grade = "partial"
	GradeError      Grade = "error"
)

// gradeRank 决定同一轮记录的排列顺序，数值越小越靠前。
//
// 入池只看 healthy（见 Store.StoreRound），其余分级的先后只影响摘要与日志里的展示顺序。
var gradeRank = map[Grade]int{
	GradeHealthy: 0, GradeWeak: 1, GradeMismatch: 2, GradePartial: 3,
	GradeOverloaded: 4, GradeBlocked: 5, GradeError: 6,
}

const (
	// probeText 是探针的输入内容，越短越省 token。
	probeText = "ping"
	// maxStateBytes 是 state 作为头值的长度上限，超过的一律丢弃。
	maxStateBytes = 8192
	// maxSSEBytes 是单次探针最多读取的 SSE 字节数，兜底防止流不结束。
	maxSSEBytes = 1 << 20
	// maxSSELineBytes 是单行 SSE 的长度上限。
	maxSSELineBytes = 512 << 10
	// maxDetailBytes 是记录到状态里的错误文本长度上限。
	maxDetailBytes = 200
)

// ClientProvider 提供按代理地址复用的 HTTP 客户端，由 transport.Pool 实现。
type ClientProvider interface {
	Client(proxyURL string) (*http.Client, error)
}

// record 是一次探针的结果。proxy 是完整代理 URL，为空表示直连。
type record struct {
	grade  Grade
	state  string
	length int
	proxy  string
	status int
	detail string
}

// prober 按一份票池配置发探针。它是只读的，可被并发调用。
type prober struct {
	clients ClientProvider
	config  pluginconfig.Ticket
	// header 是要从响应里取走的票据头名。
	header string
}

// probe 发一次撞票探针并给结果分级。
//
// 探针走的是与真实请求同一套连接参数；proxyURL 非空时按它的协议
// （http/https/socks5/socks5h）拨号，为空则直连。
func (p *prober) probe(ctx context.Context, cred Credential, model, proxyURL string) record {
	out := record{proxy: proxyURL, grade: GradeError}

	client, err := p.clients.Client(proxyURL)
	if err != nil {
		out.detail = detailOf(err.Error())
		return out
	}

	payload, err := json.Marshal(probeBody(model, p.config.ProbeEffort))
	if err != nil {
		out.detail = detailOf(err.Error())
		return out
	}

	timeout := time.Duration(p.config.ProbeTimeoutSeconds) * time.Second
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost,
		p.endpoint(cred), bytes.NewReader(payload))
	if err != nil {
		out.detail = detailOf(err.Error())
		return out
	}
	request.Header.Set("Authorization", cred.Authorization)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("User-Agent", p.userAgent(cred))
	if cred.AccountHeader != "" {
		request.Header.Set("ChatGpt-Account-Id", cred.AccountHeader)
	}

	response, err := client.Do(request)
	if err != nil {
		// 连响应都没拿到：代理或网络问题，不是账号问题。
		out.detail = detailOf(err.Error())
		return out
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		response.Body.Close()
	}()

	out.status = response.StatusCode
	out.state = sanitizeState(headerLookup(response.Header, p.header))
	out.length = len(out.state)

	if response.StatusCode != http.StatusOK {
		if out.state != "" {
			out.grade = GradeWeak
		} else {
			out.grade = GradeBlocked
		}
		body, _ := io.ReadAll(io.LimitReader(response.Body, maxDetailBytes))
		out.detail = detailOf(string(body))
		return out
	}

	stream := readSSE(response.Body)
	if stream.overloaded {
		out.grade = GradeOverloaded
		out.detail = detailOf(stream.errorMessage)
		return out
	}
	if stream.hasText || (stream.completed && !stream.completedFailed) {
		// 满血判定：有产出，且长度等于目标值。312 等 premium 池已验收确认降智。
		if p.config.TargetStateLength == 0 || out.length == p.config.TargetStateLength {
			out.grade = GradeHealthy
		} else {
			out.grade = GradeMismatch
		}
		return out
	}
	out.grade = GradePartial
	out.detail = detailOf(stream.errorMessage)
	return out
}

func (p *prober) endpoint(cred Credential) string {
	base := cred.GatewayBase
	if base == "" {
		base = p.config.GatewayBaseURL
	}
	return strings.TrimRight(base, "/") + "/responses"
}

func (p *prober) userAgent(cred Credential) string {
	if cred.UserAgent != "" {
		return cred.UserAgent
	}
	return p.config.UserAgent
}

// probeBody 构造探针请求体。effort=none 时不带 reasoning 字段。
func probeBody(model, effort string) map[string]any {
	body := map[string]any{
		"model":  model,
		"stream": true,
		"store":  false,
		"input": []any{
			map[string]any{"role": "user", "content": probeText},
		},
	}
	if effort != "" && effort != pluginconfig.EffortNone {
		body["reasoning"] = map[string]any{"effort": effort}
	}
	return body
}

// sseResult 是读 SSE 流得到的判定素材。
type sseResult struct {
	hasText         bool
	completed       bool
	completedFailed bool
	overloaded      bool
	errorMessage    string
}

// readSSE 读 SSE 流，分级一确认就立刻返回以省 token：
//   - 收到 overloaded error → 立即停；
//   - 收到首个 output delta → 节点确认可用，立即停；
//   - 收到 response.completed / response.failed → 立即停。
//
// 整体超时由调用方的 context 控制，这里只兜住字节数上限。
func readSSE(body io.Reader) sseResult {
	var out sseResult
	scanner := bufio.NewScanner(io.LimitReader(body, maxSSEBytes))
	scanner.Buffer(make([]byte, 0, 8<<10), maxSSELineBytes)

	event := ""
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		// SSE 规定字段名后的冒号可以不跟空格，两种写法都要认。
		if rest, found := strings.CutPrefix(line, "event:"); found {
			event = strings.TrimSpace(rest)
			continue
		}
		data, found := strings.CutPrefix(line, "data:")
		if !found {
			continue
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			continue
		}
		switch event {
		case "response.completed":
			out.completed = true
			out.completedFailed = responseHasError(payload["response"])
			return out
		case "response.failed":
			// 上游已经宣告失败，即使 response 对象里没附带 error 也不能当成满血。
			out.completed = true
			out.completedFailed = true
			return out
		case "error":
			code, message := parseErrorObject(payload["error"])
			out.errorMessage = message
			if isOverloaded(code, message) {
				out.overloaded = true
				return out
			}
		case "response.output_text.delta":
			out.hasText = true
			return out
		}
	}
	return out
}

// responseHasError 判断 response.completed/failed 里的 response 对象是否带 error。
func responseHasError(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var response struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return false
	}
	return len(response.Error) > 0 && string(response.Error) != "null"
}

func parseErrorObject(raw json.RawMessage) (string, string) {
	if len(raw) == 0 {
		return "", ""
	}
	var object struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		return "", ""
	}
	return object.Code, object.Message
}

// isOverloaded 与参考实现一致：code 或 message 里出现 overload 就算降智。
func isOverloaded(code, message string) bool {
	return strings.Contains(strings.ToLower(code), "overload") ||
		strings.Contains(strings.ToLower(message), "overload")
}

// sanitizeState 挡掉不能安全当作头值注入的 state。
//
// 这个值会被原样写进发往上游的请求头，含 CR/LF/NUL 就是响应头注入。
func sanitizeState(value string) string {
	if value == "" || len(value) > maxStateBytes {
		return ""
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return ""
	}
	return value
}

// sortRecords 过滤出带 state 的记录并按 (分级, 长度降序) 排序。
func sortRecords(records []record) []record {
	kept := make([]record, 0, len(records))
	for _, item := range records {
		if item.state == "" {
			continue
		}
		kept = append(kept, item)
	}
	sort.SliceStable(kept, func(i, j int) bool {
		left, right := gradeRank[kept[i].grade], gradeRank[kept[j].grade]
		if left != right {
			return left < right
		}
		return kept[i].length > kept[j].length
	})
	return kept
}

// truncateDetail 压掉换行与过长内容，保证能安全写进状态与日志。
func truncateDetail(text string) string {
	text = strings.Map(func(char rune) rune {
		if char < 0x20 || char == 0x7f {
			return ' '
		}
		return char
	}, text)
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > maxDetailBytes {
		return text[:maxDetailBytes] + "…"
	}
	return text
}

var (
	// detailURLPattern 匹配错误文本里的完整 URL：net/http 与 x/net 的错误会把
	// 网关地址、代理地址原样带在 `Post "https://…"`、`proxyconnect` 这类前缀之后。
	detailURLPattern = regexp.MustCompile(`(?i)\b(?:https?|socks5h?)://[^\s"'<>]+`)
	// detailAddrPattern 匹配裸的地址：IPv4（可带端口）、[IPv6]（可带端口）、带端口的域名。
	// 域名必须带端口才算地址，免得把 response.output_text 这类点分标识也打了码。
	detailAddrPattern = regexp.MustCompile(
		`(?:\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?|\[[0-9A-Fa-f:.]+\](?::\d{1,5})?|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}:\d{1,5})`)
)

// scrubDetail 把探针错误文本里的 URL 与 host:port 打码。
//
// 这些文本来自第三方（net/http、代理库、上游返回的错误体），会原样进看板与
// 健康检查输出；里面的代理地址是管理员的付费资源，网关地址也可能是私有中转。
// 打码规则与 proxypool.MaskProxy 一致：IPv4 只留前两段，其它主机只留前两个字符。
func scrubDetail(text string) string {
	text = detailURLPattern.ReplaceAllStringFunc(text, func(match string) string {
		// 错误文本里 URL 后面常紧跟着 `:` 或 `,`，它们不是 URL 的一部分，打码后要原样留下。
		trimmed := strings.TrimRight(match, ":.,;)]")
		return proxypool.MaskProxy(trimmed) + match[len(trimmed):]
	})
	return detailAddrPattern.ReplaceAllStringFunc(text, func(match string) string {
		host, port, err := net.SplitHostPort(match)
		if err != nil {
			return proxypool.MaskHost(strings.Trim(match, "[]"))
		}
		return net.JoinHostPort(proxypool.MaskHost(strings.Trim(host, "[]")), port)
	})
}

// detailOf 是探针结果里 detail 字段的唯一入口：先打码再截断。
func detailOf(text string) string {
	return truncateDetail(scrubDetail(text))
}

// describeRecord 给出一行人类可读的探针结果，用于日志与看板。
// 刻意不含 state 本身：它是凭据级的东西，不进日志；代理地址同样按段打码。
func describeRecord(item record) string {
	target := "direct"
	if item.proxy != "" {
		target = maskAddr(item.proxy)
	}
	return fmt.Sprintf("%s=%s(%d/%d)", target, item.grade, item.status, item.length)
}

// summarizeRecords 汇总一轮各出口的分级，例如
// "direct=mismatch(200/312) 1.2.*.*:1080=healthy(200/292)"。
//
// 一轮没补到票时这行会进看板：管理员据此区分「长度不符」「被挡」「连不上」。
func summarizeRecords(records []record) string {
	parts := make([]string, 0, len(records))
	for _, item := range records {
		parts = append(parts, describeRecord(item))
	}
	return truncateDetail(strings.Join(parts, " "))
}

// maskAddr 把代理地址压成能进日志的形式（去凭据、IPv4 打码），
// 复用代理库的实现，保证看板与日志里的写法一致。
func maskAddr(proxyURL string) string {
	return proxypool.MaskProxy(proxyURL)
}
