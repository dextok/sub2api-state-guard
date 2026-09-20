package transport

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// countingReader 记录实际从上游请求体读走了多少字节，用来确认嗅探不会把整个体读完。
type countingReader struct {
	reader   *strings.Reader
	read     int
	closed   bool
	chunkCap int // >0 时每次最多返回这么多字节，模拟小分块上传
	failWith error
}

func (c *countingReader) Read(target []byte) (int, error) {
	if c.chunkCap > 0 && len(target) > c.chunkCap {
		target = target[:c.chunkCap]
	}
	read, err := c.reader.Read(target)
	c.read += read
	if err == io.EOF && c.failWith != nil {
		err = c.failWith
	}
	return read, err
}

func (c *countingReader) Close() error {
	c.closed = true
	return nil
}

func newBody(payload string) *countingReader {
	return &countingReader{reader: strings.NewReader(payload)}
}

// drain 读完 peekModel 返回的请求体，确认字节与原始请求体完全一致。
func drain(t *testing.T, body io.ReadCloser) string {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("重放请求体失败: %v", err)
	}
	return string(data)
}

func TestPeekModelFindsTopLevelModel(t *testing.T) {
	cases := map[string]struct {
		payload string
		want    string
	}{
		"第一个字段":     {payload: `{"model":"gpt-5-codex","stream":true}`, want: "gpt-5-codex"},
		"嵌套结构之后":    {payload: `{"input":[{"role":"user","content":[{"type":"text"}]}],"tools":{"a":[1,2]},"model":"gpt-5"}`, want: "gpt-5"},
		"空对象与空数组之后": {payload: `{"a":{},"b":[],"model":"gpt-5"}`, want: "gpt-5"},
		"带转义与中文":    {payload: `{"prompt":"他说\"你好\"","model":"gpt-5-codex"}`, want: "gpt-5-codex"},
		"模型名带空白":    {payload: `{"model":"  gpt-5-codex  "}`, want: "gpt-5-codex"},
		"顶层没有模型":    {payload: `{"stream":true}`, want: ""},
		"只有嵌套的同名字段": {payload: `{"tools":[{"model":"gpt-4"}]}`, want: ""},
		"模型不是字符串":   {payload: `{"model":123}`, want: ""},
		"模型是空串":     {payload: `{"model":""}`, want: ""},
		"顶层是数组":     {payload: `[{"model":"gpt-5"}]`, want: ""},
		"根本不是 JSON": {payload: "\x00\x01binary-upload", want: ""},
		"空请求体":      {payload: ``, want: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			body := newBody(tc.payload)
			model, replayed := peekModel(body, 256<<10)
			if model != tc.want {
				t.Fatalf("解析出的模型 = %q，期望 %q", model, tc.want)
			}
			if got := drain(t, replayed); got != tc.payload {
				t.Fatalf("重放的请求体 = %q，期望逐字节一致", got)
			}
		})
	}
}

// 找到模型名就立刻停手：不能为了嗅探把整个大请求体攒进内存。
func TestPeekModelStopsReadingOnceDecided(t *testing.T) {
	payload := `{"model":"gpt-5-codex","input":"` + strings.Repeat("x", 200<<10) + `"}`
	body := newBody(payload)

	model, replayed := peekModel(body, 256<<10)
	if model != "gpt-5-codex" {
		t.Fatalf("解析出的模型 = %q", model)
	}
	if body.read > 2*sniffChunkSize {
		t.Fatalf("嗅探读走了 %d 字节，模型名在开头时不应读这么多", body.read)
	}
	if got := drain(t, replayed); got != payload {
		t.Fatal("重放的请求体与原始请求体不一致")
	}
}

// 宿主按小分块推送请求体时，跨分块的 JSON 仍要能拼出模型名。
func TestPeekModelHandlesTinyChunks(t *testing.T) {
	payload := `{"stream":true,"model":"gpt-5-codex"}`
	body := &countingReader{reader: strings.NewReader(payload), chunkCap: 1}

	model, replayed := peekModel(body, 256<<10)
	if model != "gpt-5-codex" {
		t.Fatalf("解析出的模型 = %q", model)
	}
	if got := drain(t, replayed); got != payload {
		t.Fatalf("重放的请求体 = %q", got)
	}
}

// 超过嗅探上限仍没结论时放弃解析，按"未知模型"处理，请求体照常转发。
func TestPeekModelGivesUpBeyondLimit(t *testing.T) {
	payload := `{"input":"` + strings.Repeat("x", 4096) + `","model":"gpt-5-codex"}`
	body := newBody(payload)

	model, replayed := peekModel(body, 1024)
	if model != "" {
		t.Fatalf("超过上限应当放弃解析，得到 %q", model)
	}
	if got := drain(t, replayed); got != payload {
		t.Fatal("放弃解析后请求体仍必须完整重放")
	}
}

// 请求体被截断（客户端中断）时不能误判，也不能把错误吞掉。
func TestPeekModelWithTruncatedBody(t *testing.T) {
	payload := `{"input":"unfinished`
	body := newBody(payload)

	model, replayed := peekModel(body, 256<<10)
	if model != "" {
		t.Fatalf("截断的 JSON 不应解析出模型，得到 %q", model)
	}
	if got := drain(t, replayed); got != payload {
		t.Fatalf("重放的请求体 = %q", got)
	}
}

// 预读阶段拿到的读取错误必须在缓冲重放完之后原样交给调用方。
func TestReplayBodyPropagatesSourceError(t *testing.T) {
	failure := io.ErrUnexpectedEOF
	body := &countingReader{reader: strings.NewReader(`{"model":"gpt-5-codex"`), failWith: failure}

	model, replayed := peekModel(body, 256<<10)
	if model != "gpt-5-codex" {
		t.Fatalf("解析出的模型 = %q", model)
	}
	data, err := io.ReadAll(replayed)
	if err != failure {
		t.Fatalf("重放应当原样抛出上游错误，得到 %v", err)
	}
	if string(data) != `{"model":"gpt-5-codex"` {
		t.Fatalf("错误之前的字节仍应被重放，得到 %q", data)
	}
	if err := replayed.Close(); err != nil {
		t.Fatalf("Close 出错: %v", err)
	}
	if !body.closed {
		t.Fatal("Close 应当传递给原始请求体")
	}
}

func TestPeekModelWithNilBodyOrZeroLimit(t *testing.T) {
	if model, body := peekModel(nil, 1024); model != "" || body != nil {
		t.Fatalf("nil 请求体应当原样返回，得到 %q/%v", model, body)
	}
	original := newBody(`{"model":"gpt-5-codex"}`)
	model, body := peekModel(original, 0)
	if model != "" {
		t.Fatalf("上限为 0 时不应嗅探，得到 %q", model)
	}
	if body != io.ReadCloser(original) {
		t.Fatal("不嗅探时应当原样返回请求体，避免多包一层")
	}
}

// 模型名来自客户端请求体：控制字符要剔除，超长的直接丢弃。
func TestSanitizeModelName(t *testing.T) {
	cases := map[string]string{
		"gpt-5-codex":                            "gpt-5-codex",
		"  gpt-5-codex\t":                        "gpt-5-codex",
		"gpt\x005-codex":                         "gpt5-codex",
		"gpt-5\r\ncodex":                         "gpt-5codex",
		strings.Repeat("m", maxModelNameBytes):   strings.Repeat("m", maxModelNameBytes),
		strings.Repeat("m", maxModelNameBytes+1): "",
		"":                                       "",
		"gpt-5-codex-中文":                         "gpt-5-codex-中文",
	}
	for input, want := range cases {
		if got := sanitizeModelName(input); got != want {
			t.Fatalf("sanitizeModelName(%q) = %q，期望 %q", input, got, want)
		}
	}
	// 净化后的名字绝不能带 CR/LF：它会被写进日志与撞票探针的请求体。
	if got := sanitizeModelName("a\nb"); strings.ContainsAny(got, "\r\n") {
		t.Fatalf("净化结果仍含换行: %q", got)
	}
}

func TestIsJSONBody(t *testing.T) {
	cases := map[string]bool{
		"application/json":                  true,
		"Application/JSON; charset=utf-8":   true,
		"application/vnd.openai+json":       true,
		"application/json-patch+json":       true,
		" application/json ":                true,
		"text/event-stream":                 false,
		"application/octet-stream":          false,
		"multipart/form-data; boundary=abc": false,
		"":                                  false,
		"application/jsonish":               false,
	}
	for value, want := range cases {
		header := http.Header{}
		if value != "" {
			// 键名大小写由宿主决定，这里刻意用非规范形式。
			header["content-type"] = []string{value}
		}
		if got := isJSONBody(header); got != want {
			t.Fatalf("isJSONBody(%q) = %v，期望 %v", value, got, want)
		}
	}
}
