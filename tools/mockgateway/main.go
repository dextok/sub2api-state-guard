// Command mockgateway 是撞票链路的本地假桩，一个进程同时提供两样东西：
//
//	假 Codex 网关   POST {任意前缀}/responses —— 回 SSE 流并带上 x-codex-turn-state 头，
//	                长度、分级、失败形态都可以用命令行开关摆出来；
//	真 SOCKS5 出口  一组本地端口，经代理撞票能真的跑通。
//
// 用它可以在不碰真上游、不花代理钱的前提下把票池、代理库与实时看板整条链路跑一遍。
//
//	# 默认：292 长度的满血票 + 4 个本地 socks5 出口
//	go run ./tools/mockgateway
//
//	# 让某些模型撞出 312（premium 池，判定为长度不符）与过载
//	go run ./tools/mockgateway -mismatch-models gpt-6-astra -overloaded-models gpt-5.6-luna
//
// 插件侧对应改两处配置：网关填 http://127.0.0.1:9701/backend-api/codex，
// 代理列表把启动日志里打印的 socks5://127.0.0.1:97xx 逐条粘进去。
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// maxRequestBytes 是读取探针请求体的上限，只为取出 model 字段。
	maxRequestBytes = 64 << 10
	// maxSocks5Ports 限制本地出口个数，避免手滑开出几千个监听。
	maxSocks5Ports = 64
	// socks5HandshakeTimeout 是 SOCKS5 握手的整体超时。
	socks5HandshakeTimeout = 10 * time.Second
	// socks5DialTimeout 是代理向目标建连的超时。
	socks5DialTimeout = 10 * time.Second
	// statePreviewBytes 是日志里展示 state 开头多少字节。
	statePreviewBytes = 24
)

// SSE 形态。
const (
	sseOK         = "ok"
	sseOverloaded = "overloaded"
	ssePartial    = "partial"
	sseFailed     = "failed"
)

type server struct {
	stateLengths     []int
	mismatchModels   map[string]struct{}
	mismatchLength   int
	overloadedModels map[string]struct{}
	// downgradeModels 把「请求的模型」映射成「SSE 里自报的模型」，用来模拟上游的
	// 模型降级（请求 gpt-6-astra，实际由 gpt-5.6-luna 服务）。
	downgradeModels map[string]string
	noState         bool
	status          int
	sseMode         string
	delay           time.Duration
	requireAuth     bool

	mu        sync.Mutex
	probes    int
	lastProbe time.Time
	lengthSeq int
	mintSeq   int
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9701", "HTTP 监听地址（假网关）")
	socks5Base := flag.Int("socks5-base-port", 9750, "SOCKS5 出口的起始端口")
	socks5Count := flag.Int("socks5-count", 4, "SOCKS5 出口个数，0 表示不开出口（只能直连撞票）")
	stateLength := flag.String("state-length", "292", "state 头长度，逗号分隔时按请求轮换")
	mismatchModels := flag.String("mismatch-models", "", "这些模型固定返回 -mismatch-length 长度的 state（逗号分隔）")
	mismatchLength := flag.Int("mismatch-length", 312, "-mismatch-models 用的 state 长度")
	overloadedModels := flag.String("overloaded-models", "", "这些模型固定回过载事件（逗号分隔）")
	downgradeModels := flag.String("downgrade", "",
		"模拟上游模型降级，形如 gpt-6-astra=gpt-5.6-luna（逗号分隔多条）："+
			"请求左边的模型时，SSE 里自报右边的模型名，state 照常返回")
	noState := flag.Bool("no-state", false, "不返回 state 头")
	status := flag.Int("status", 200, "假网关的响应状态码")
	sseMode := flag.String("sse", sseOK, "SSE 形态：ok / overloaded / partial / failed")
	delay := flag.Duration("delay", 0, "响应前的等待，用来验证探针超时")
	requireAuth := flag.Bool("require-auth", true, "没有 Authorization: Bearer 头时返回 401")
	flag.Parse()

	lengths, err := parseLengths(*stateLength)
	if err != nil {
		log.Fatalf("-state-length %v", err)
	}
	switch *sseMode {
	case sseOK, sseOverloaded, ssePartial, sseFailed:
	default:
		log.Fatalf("-sse 只支持 %s / %s / %s / %s", sseOK, sseOverloaded, ssePartial, sseFailed)
	}
	if *status < 100 || *status > 599 {
		log.Fatalf("-status 必须在 100-599 之间")
	}
	if *socks5Count < 0 || *socks5Count > maxSocks5Ports {
		log.Fatalf("-socks5-count 必须在 0-%d 之间", maxSocks5Ports)
	}

	downgrades, err := parseDowngrades(*downgradeModels)
	if err != nil {
		log.Fatalf("-downgrade %v", err)
	}

	exits, err := startSocks5(*socks5Base, *socks5Count)
	if err != nil {
		log.Fatalf("启动 SOCKS5 出口失败: %v", err)
	}

	instance := &server{
		stateLengths:     lengths,
		mismatchModels:   parseSet(*mismatchModels),
		mismatchLength:   *mismatchLength,
		overloadedModels: parseSet(*overloadedModels),
		downgradeModels:  downgrades,
		noState:          *noState,
		status:           *status,
		sseMode:          *sseMode,
		delay:            *delay,
		requireAuth:      *requireAuth,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/", instance.route)

	log.Printf("假网关   http://%s/backend-api/codex  （探针端点 …/responses）", *addr)
	log.Printf("连通测试 http://%s/healthz", *addr)
	if len(exits) == 0 {
		log.Printf("SOCKS5 出口：未开启，只能直连撞票")
	} else {
		// 直接打印成配置页能粘的形态：一行一条，复制进「添加代理」的地址框即可。
		log.Printf("SOCKS5 出口（粘进代理列表）：")
		for _, exit := range exits {
			log.Printf("  socks5://%s", exit)
		}
	}
	log.Printf("state 长度 %v，SSE %s，状态码 %d", lengths, *sseMode, *status)

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	log.Fatal(httpServer.ListenAndServe())
}

// route 把 …/responses 交给假网关，其余路径落到回显端点。
func (s *server) route(writer http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(strings.TrimSuffix(request.URL.Path, "/"), "/responses") {
		s.handleResponses(writer, request)
		return
	}
	s.handleEcho(writer, request)
}

// handleResponses 既是撞票探针的目标，也是插件转发真实请求时的上游：
// 两种流量都会被打印，注入头有没有到位一眼可见。
func (s *server) handleResponses(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "只接受 POST", http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes))
	var probe struct {
		Model     string `json:"model"`
		Reasoning struct {
			Effort string `json:"effort"`
		} `json:"reasoning"`
	}
	// 请求体畸形时按「没有模型」处理：假桩只要能继续应答，不模拟真实网关的 400。
	_ = json.Unmarshal(body, &probe)
	model := headerSafe(probe.Model)

	authorized := strings.HasPrefix(strings.ToLower(request.Header.Get("Authorization")), "bearer ")
	sequence, gap := s.note()
	incoming := describeIncomingState(request.Header.Get("X-Codex-Turn-State"))

	if s.requireAuth && !authorized {
		log.Printf("#%d %s model=%s 缺少 Authorization → 401%s", sequence, gap, model, incoming)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusUnauthorized)
		io.WriteString(writer, `{"error":{"code":"missing_authorization","message":"mockgateway 要求 Bearer 令牌"}}`)
		return
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-request.Context().Done():
			log.Printf("#%d %s model=%s 客户端在等待中断开", sequence, gap, model)
			return
		}
	}

	length := s.stateLengthFor(probe.Model)
	state := ""
	if !s.noState && length > 0 {
		state = s.mintState(model, length)
		writer.Header().Set("X-Codex-Turn-State", state)
	}

	if s.status != http.StatusOK {
		log.Printf("#%d %s model=%s effort=%s → %d state=%d%s",
			sequence, gap, model, orNone(probe.Reasoning.Effort), s.status, len(state), incoming)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(s.status)
		fmt.Fprintf(writer, `{"error":{"code":"mock_status","message":"mockgateway 按 -status %d 返回"}}`, s.status)
		return
	}

	mode := s.sseModeFor(probe.Model)
	served := s.servedModelFor(model)
	downgraded := ""
	if served != model {
		downgraded = fmt.Sprintf(" served=%s(降级)", served)
	}
	log.Printf("#%d %s model=%s effort=%s ua=%s account=%s → 200 state=%d sse=%s%s%s",
		sequence, gap, model, orNone(probe.Reasoning.Effort),
		orNone(request.Header.Get("User-Agent")),
		maskAccount(request.Header.Get("ChatGpt-Account-Id")),
		len(state), mode, downgraded, incoming)
	s.writeSSE(writer, mode, served)
}

// writeSSE 按形态写事件流。插件的读取端一确认分级就会断开，所以这里逐条 flush。
//
// servedModel 写进 response 对象的 model 字段：真实网关就是这么自报的，
// 插件靠它判断上游有没有偷偷换模型。
func (s *server) writeSSE(writer http.ResponseWriter, mode, servedModel string) {
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusOK)
	flusher, _ := writer.(http.Flusher)
	emit := func(event, data string) {
		fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event, data)
		if flusher != nil {
			flusher.Flush()
		}
	}

	model, _ := json.Marshal(servedModel)
	emit("response.created", fmt.Sprintf(
		`{"type":"response.created","response":{"id":"resp_mock","status":"in_progress","model":%s}}`, model))
	switch mode {
	case sseOverloaded:
		emit("error", `{"type":"error","error":{"code":"model_overloaded","message":"model is overloaded, try again later"}}`)
	case ssePartial:
		// 什么都不再发：插件读到流结束仍没有产出，判为「半截」。
	case sseFailed:
		emit("response.failed", `{"type":"response.failed","response":{"id":"resp_mock","status":"failed","error":{"code":"mock_failed","message":"mockgateway 按 -sse failed 返回"}}}`)
	default:
		emit("response.output_text.delta", `{"type":"response.output_text.delta","delta":"pong"}`)
		emit("response.completed", fmt.Sprintf(
			`{"type":"response.completed","response":{"id":"resp_mock","status":"completed","model":%s}}`, model))
	}
}

// handleEcho 回显收到的请求元信息，用来确认注入头有没有跟着真实请求出去。
// Authorization 只报长度：假桩也不该把令牌原文写进响应与日志。
func (s *server) handleEcho(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes))
	var payload struct {
		Model string `json:"model"`
	}
	// 同上：解析失败就回显空模型名。
	_ = json.Unmarshal(body, &payload)

	state := request.Header.Get("X-Codex-Turn-State")
	result := map[string]any{
		"method":             request.Method,
		"path":               request.URL.Path,
		"model":              headerSafe(payload.Model),
		"user_agent":         request.Header.Get("User-Agent"),
		"chatgpt_account_id": maskAccount(request.Header.Get("ChatGpt-Account-Id")),
		"authorization_len":  len(request.Header.Get("Authorization")),
		"turn_state_len":     len(state),
		"turn_state_preview": preview(state),
	}
	log.Printf("回显 %s %s model=%s%s", request.Method, request.URL.Path,
		headerSafe(payload.Model), describeIncomingState(state))
	writer.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	encoder.Encode(result)
}

// note 记一次探针，返回序号与距上次的间隔描述。
func (s *server) note() (int, string) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes++
	gap := "首次"
	if !s.lastProbe.IsZero() {
		gap = fmt.Sprintf("间隔%.1fs", now.Sub(s.lastProbe).Seconds())
	}
	s.lastProbe = now
	return s.probes, gap
}

// stateLengthFor 决定这次给多长的 state：命中 -mismatch-models 用固定长度，
// 否则按 -state-length 列表轮换。
func (s *server) stateLengthFor(model string) int {
	if _, hit := s.mismatchModels[strings.ToLower(strings.TrimSpace(model))]; hit {
		return s.mismatchLength
	}
	if len(s.stateLengths) == 1 {
		return s.stateLengths[0]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	length := s.stateLengths[s.lengthSeq%len(s.stateLengths)]
	s.lengthSeq++
	return length
}

func (s *server) sseModeFor(model string) string {
	if _, hit := s.overloadedModels[strings.ToLower(strings.TrimSpace(model))]; hit {
		return sseOverloaded
	}
	return s.sseMode
}

// servedModelFor 返回 SSE 里该自报哪个模型：命中 -downgrade 就报映射后的那个，
// 否则原样报回请求的模型。
func (s *server) servedModelFor(model string) string {
	if served, hit := s.downgradeModels[strings.ToLower(strings.TrimSpace(model))]; hit {
		return served
	}
	return model
}

// mintState 铸一张长度精确的假票，开头带上模型名，方便一眼看出有没有拿错模型的票。
func (s *server) mintState(model string, length int) string {
	if length <= 0 {
		return ""
	}
	s.mu.Lock()
	s.mintSeq++
	seq := s.mintSeq
	s.mu.Unlock()

	const filler = "0123456789abcdefghijklmnopqrstuvwxyz"
	var builder strings.Builder
	builder.Grow(length)
	builder.WriteString(fmt.Sprintf("og-%s-%d-", model, seq))
	for index := 0; builder.Len() < length; index++ {
		builder.WriteByte(filler[index%len(filler)])
	}
	return builder.String()[:length]
}

// startSocks5 在 base 起的连续端口上各开一个 SOCKS5 出口，返回它们的 ip:port。
func startSocks5(base, count int) ([]string, error) {
	exits := make([]string, 0, count)
	for index := 0; index < count; index++ {
		port := base + index
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("端口 %d 越界", port)
		}
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		go acceptSocks5(listener, addr)
		exits = append(exits, addr)
	}
	return exits, nil
}

func acceptSocks5(listener net.Listener, label string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("socks5 %s 停止接受连接: %v", label, err)
			return
		}
		go func() {
			defer conn.Close()
			if err := serveSocks5(conn, label); err != nil {
				log.Printf("socks5 %s 会话失败: %v", label, err)
			}
		}()
	}
}

// serveSocks5 实现最小可用的 SOCKS5 CONNECT：不认证、不做访问控制。
// 它只监听回环地址，仅用于本地联调。
func serveSocks5(conn net.Conn, label string) error {
	conn.SetDeadline(time.Now().Add(socks5HandshakeTimeout))

	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return err
	}
	if greeting[0] != 0x05 {
		return errors.New("不是 SOCKS5 握手")
	}
	if _, err := io.ReadFull(conn, make([]byte, int(greeting[1]))); err != nil {
		return err
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return err
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[0] != 0x05 {
		return errors.New("请求版本不是 5")
	}
	if head[1] != 0x01 {
		conn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return fmt.Errorf("只支持 CONNECT，收到命令 %d", head[1])
	}

	host, err := readSocks5Host(conn, head[3])
	if err != nil {
		conn.Write([]byte{0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return err
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes))))

	upstream, err := net.DialTimeout("tcp", target, socks5DialTimeout)
	if err != nil {
		conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return fmt.Errorf("连接 %s 失败: %w", target, err)
	}
	defer upstream.Close()
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	log.Printf("socks5 %s → %s", label, target)

	// 握手完成后去掉截止时间，长连接与流式响应才不会被从中间掐断。
	conn.SetDeadline(time.Time{})
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		io.Copy(upstream, conn)
		closeWrite(upstream)
	}()
	go func() {
		defer group.Done()
		io.Copy(conn, upstream)
		closeWrite(conn)
	}()
	group.Wait()
	return nil
}

func readSocks5Host(conn net.Conn, kind byte) (string, error) {
	switch kind {
	case 0x01:
		buffer := make([]byte, 4)
		if _, err := io.ReadFull(conn, buffer); err != nil {
			return "", err
		}
		return net.IP(buffer).String(), nil
	case 0x03:
		size := make([]byte, 1)
		if _, err := io.ReadFull(conn, size); err != nil {
			return "", err
		}
		buffer := make([]byte, int(size[0]))
		if _, err := io.ReadFull(conn, buffer); err != nil {
			return "", err
		}
		return string(buffer), nil
	case 0x04:
		buffer := make([]byte, 16)
		if _, err := io.ReadFull(conn, buffer); err != nil {
			return "", err
		}
		return net.IP(buffer).String(), nil
	default:
		return "", fmt.Errorf("不支持的地址类型 %d", kind)
	}
}

func closeWrite(conn net.Conn) {
	if closer, ok := conn.(interface{ CloseWrite() error }); ok {
		closer.CloseWrite()
		return
	}
	conn.Close()
}

func parseLengths(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		value, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("里的 %q 不是整数", trimmed)
		}
		if value < 0 || value > 8192 {
			return nil, fmt.Errorf("里的 %d 越界，必须在 0-8192 之间", value)
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, errors.New("不能为空")
	}
	return out, nil
}

func parseSet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.ToLower(strings.TrimSpace(part))
		if trimmed != "" {
			out[trimmed] = struct{}{}
		}
	}
	return out
}

// parseDowngrades 解析 -downgrade 的 "请求模型=自报模型" 列表。
func parseDowngrades(raw string) (map[string]string, error) {
	out := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		from, to, found := strings.Cut(trimmed, "=")
		from = strings.ToLower(strings.TrimSpace(from))
		to = strings.TrimSpace(to)
		if !found || from == "" || to == "" {
			return nil, fmt.Errorf("%q 不是 请求模型=自报模型 的形式", trimmed)
		}
		out[from] = headerSafe(to)
	}
	return out, nil
}

// headerSafe 把模型名收敛到能安全塞进头值与日志的字符集。
func headerSafe(model string) string {
	trimmed := strings.TrimSpace(model)
	if trimmed == "" {
		return "unknown"
	}
	if len(trimmed) > 40 {
		trimmed = trimmed[:40]
	}
	var builder strings.Builder
	for index := 0; index < len(trimmed); index++ {
		char := trimmed[index]
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z',
			char >= '0' && char <= '9', char == '-', char == '.', char == '_':
			builder.WriteByte(char)
		default:
			builder.WriteByte('-')
		}
	}
	return builder.String()
}

// maskAccount 只留账号头的前 8 个字符：它是账号标识，没必要整条打进日志。
func maskAccount(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "无"
	}
	if len(trimmed) <= 8 {
		return trimmed
	}
	return trimmed[:8] + "…"
}

// describeIncomingState 描述请求「带进来」的 state 头——插件注入是否生效看这一段。
func describeIncomingState(value string) string {
	if value == "" {
		return ""
	}
	return fmt.Sprintf(" 注入头=%d字节(%s)", len(value), preview(value))
}

func preview(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= statePreviewBytes {
		return value
	}
	return value[:statePreviewBytes] + "…"
}

func orNone(value string) string {
	if strings.TrimSpace(value) == "" {
		return "无"
	}
	return value
}
