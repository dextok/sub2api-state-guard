package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/pluginconfig"
)

// state 头按模型铸造，不同模型之间不能复用，所以注入前必须知道本次请求用的是哪个模型。
//
// 宿主的 ForwardRequestStart 帧里没有模型字段（backend/pkg/pluginapi/v1/plugin.proto），
// 模型名只存在于出站请求体的 JSON 里（/responses 的 "model"）。因此这里在真正发出
// 请求之前先缓冲请求体开头的一段，扫出顶层 "model"，再把缓冲内容原样接回请求体继续流式转发：
//   - 只读开头，找到就停，通常一个 32 KB 分块就够，不会把整个请求体攒起来；
//   - 缓冲的字节会原样重放，Content-Length 与分块语义都不受影响；
//   - 嗅探只在账号确实被接管时进行，纯透传账号没有任何额外开销。

// maxModelNameBytes 限制取用的模型名长度。模型名来自客户端请求体，属于不可信输入；
// 上限与配置里 models 的规则相同，超过的名字既不会用来取票，也不会被自动跟踪。
const maxModelNameBytes = pluginconfig.MaxModelNameBytes

// sniffChunkSize 是预读的单次读取粒度，与宿主发送请求体的分块大小一致。
const sniffChunkSize = 32 * 1024

// peekModel 预读请求体开头，返回顶层 "model" 字段与可继续流式读取的请求体。
//
// 返回的第二个值一定可以代替原请求体使用（即便返回空模型名）；body 为 nil 时原样返回 nil。
func peekModel(body io.ReadCloser, limit int) (string, io.ReadCloser) {
	if body == nil || limit <= 0 {
		return "", body
	}

	buffer := make([]byte, 0, sniffChunkSize)
	chunk := make([]byte, sniffChunkSize)
	for len(buffer) < limit {
		size := limit - len(buffer)
		if size > len(chunk) {
			size = len(chunk)
		}
		read, err := body.Read(chunk[:size])
		if read > 0 {
			buffer = append(buffer, chunk[:read]...)
			if model, incomplete := scanModel(buffer); !incomplete {
				// 已经能定论（找到了，或确定顶层没有 model 字段），不再多读一个字节。
				return sanitizeModelName(model), &replayBody{buffered: buffer, source: body}
			}
		}
		if err != nil {
			model, _ := scanModel(buffer)
			return sanitizeModelName(model), &replayBody{buffered: buffer, source: body, err: err}
		}
	}
	// 到达嗅探上限仍未定论：放弃解析，按"未知模型"处理，请求体照常转发。
	model, _ := scanModel(buffer)
	return sanitizeModelName(model), &replayBody{buffered: buffer, source: body}
}

// replayBody 先吐出预读缓冲，再接着读原始请求体。
type replayBody struct {
	buffered []byte
	offset   int
	source   io.ReadCloser
	// err 是预读阶段已经拿到的终止错误（含 io.EOF），缓冲读完后原样重放。
	err error
}

func (r *replayBody) Read(target []byte) (int, error) {
	if r.offset < len(r.buffered) {
		copied := copy(target, r.buffered[r.offset:])
		r.offset += copied
		return copied, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	return r.source.Read(target)
}

func (r *replayBody) Close() error {
	return r.source.Close()
}

// scanModel 在（可能被截断的）JSON 字节里找顶层 "model" 字段。
//
// 第二个返回值表示"数据还不够、再多给一些可能就有结论了"。只看顶层字段：
// /responses 与 /chat/completions 的模型名都在顶层，往嵌套结构里猜会取到工具或
// 子请求里的模型名。
func scanModel(data []byte) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return "", incompleteJSON(err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		// 不是 JSON 对象（表单、二进制、SSE 上传……），没有模型名可言。
		return "", false
	}
	for {
		token, err = decoder.Token()
		if err != nil {
			return "", incompleteJSON(err)
		}
		if _, ok := token.(json.Delim); ok {
			// 顶层对象读完了，没有 model 字段。
			return "", false
		}
		key, ok := token.(string)
		if !ok {
			return "", false
		}
		if key == "model" {
			token, err = decoder.Token()
			if err != nil {
				return "", incompleteJSON(err)
			}
			if value, ok := token.(string); ok {
				return value, false
			}
			return "", false
		}
		if err := skipJSONValue(decoder); err != nil {
			return "", incompleteJSON(err)
		}
	}
}

// skipJSONValue 跳过一个完整的值，包括任意深度的对象与数组。
func skipJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delimiter == '}' || delimiter == ']' {
		return nil
	}
	depth := 1
	for depth > 0 {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

// incompleteJSON 区分"数据被截断"与"确实不是我们要的结构"。
func incompleteJSON(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// sanitizeModelName 净化模型名：它来自客户端请求体，会成为票池的键，也会出现在
// 日志、看板与撞票探针的请求体里，必须先去掉控制字符并限长。
func sanitizeModelName(model string) string {
	cleaned := strings.Map(func(char rune) rune {
		if char < 0x20 || char == 0x7f {
			return -1
		}
		return char
	}, strings.TrimSpace(model))
	if len(cleaned) > maxModelNameBytes {
		return ""
	}
	return cleaned
}

// isJSONBody 判断请求体是否值得嗅探。非 JSON 的出站请求（例如上传）不含模型名。
func isJSONBody(header http.Header) bool {
	value, _ := headerLookup(header, "Content-Type")
	value = strings.ToLower(value)
	if index := strings.IndexByte(value, ';'); index >= 0 {
		value = value[:index]
	}
	value = strings.TrimSpace(value)
	return value == "application/json" || strings.HasSuffix(value, "+json")
}
