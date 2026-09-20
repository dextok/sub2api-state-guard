package runtime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/transport"
)

// CodeProtocolError 表示宿主发来的帧序列不符合协议。
const CodeProtocolError = "OVERLOAD_GUARD_PROTOCOL_ERROR"

// responseChunkSize 与宿主发送请求体时的分块大小一致。
const responseChunkSize = 32 * 1024

// forwardStream 是 Forward 的双向流。单独起个别名，测试里可以直接实现它。
type forwardStream = grpc.BidiStreamingServer[pluginv1.ForwardRequest, pluginv1.ForwardResponse]

// Forward 承载一次出站请求。
//
// 帧顺序严格遵守协议：收到 start → 按需接收 body_chunk 直到 body_end；
// 回送 start → body_chunk* → end，任何失败都以 error 帧结束。
// 一旦 error 帧成功发出，RPC 本身就算正常结束（返回 nil），错误语义由帧携带。
func (s *Server) Forward(stream forwardStream) error {
	first, err := stream.Recv()
	if err != nil {
		// 连 start 帧都没收到：流已经断了，回错误帧也没人读。
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	start := first.GetStart()
	if start == nil {
		return sendErrorFrame(stream, &transport.Error{
			Code:        CodeProtocolError,
			Message:     "转发流的首帧必须是 start",
			RequestSent: false,
		})
	}

	current := s.state()
	if current == nil {
		go drainRequestFrames(stream)
		return sendErrorFrame(stream, &transport.Error{
			Code:        transport.CodeConfigError,
			Message:     "插件尚未就绪",
			RequestSent: false,
		})
	}

	requestCtx, cancel := requestContext(stream.Context(), current.forwarder)
	defer cancel()

	body := requestBody(stream, start.GetHasBody())
	if body != nil {
		// Forwarder 在失败路径上负责读空并关闭请求体；这里兜住成功路径之后的残留，
		// 让负责接帧的 goroutine 一定能退出。
		defer func() { _ = body.Close() }()
	}

	startedAt := time.Now()
	response, forwardErr := current.forwarder.Do(requestCtx, transport.Request{
		Method:        start.GetMethod(),
		URL:           start.GetUrl(),
		Host:          start.GetHost(),
		Header:        headerFromFrame(start.GetHeaders()),
		ProxyURL:      start.GetProxyUrl(),
		AccountID:     start.GetAccountId(),
		ContentLength: start.GetContentLength(),
		HasBody:       start.GetHasBody(),
		Body:          body,
	})
	if forwardErr != nil {
		return sendErrorFrame(stream, forwardErr)
	}
	defer func() { _ = response.Body.Close() }()

	if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{
		Start: &pluginv1.ForwardResponseStart{
			StatusCode:    int32(response.StatusCode),
			Status:        response.Status,
			Protocol:      response.Proto,
			ProtocolMajor: int32(response.ProtoMajor),
			ProtocolMinor: int32(response.ProtoMinor),
			Headers:       headerToFrame(response.Header),
			ContentLength: response.ContentLength,
		},
	}}); err != nil {
		return err
	}

	received, streamErr := streamResponseBody(stream, response.Body)
	if streamErr != nil {
		if sendFailed(streamErr) {
			return streamErr
		}
		// 响应头已经发出，只能用 error 帧告知宿主响应被截断。
		return sendErrorFrame(stream, &transport.Error{
			Code:        transport.CodeUpstreamError,
			Message:     "读取上游响应体失败: " + transport.SanitizeMessage(streamErr.Error()),
			RequestSent: true,
		})
	}

	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{
		End: &pluginv1.ForwardResponseEnd{
			BytesReceived: received,
			DurationMs:    time.Since(startedAt).Milliseconds(),
		},
	}})
}

// requestContext 把配置的整体超时叠加到流 context 上。
// request_timeout_seconds 默认 0（不限制），否则流式响应会被从中间掐断。
func requestContext(parent context.Context, forwarder *transport.Forwarder) (context.Context, context.CancelFunc) {
	if timeout, ok := forwarder.RequestTimeout(); ok {
		return context.WithTimeout(parent, time.Duration(timeout)*time.Second)
	}
	return context.WithCancel(parent)
}

// requestBody 把后续的 body_chunk 帧接成一个流式请求体。
//
// 宿主与插件的请求体是并发的：宿主一边发体一边等响应头，所以这里必须边收边转发，
// 不能先攒齐再发出，否则大体请求与上传型接口都会多等一个 RTT 以上。
func requestBody(stream forwardStream, hasBody bool) io.ReadCloser {
	if !hasBody {
		// 宿主即便没有请求体也会发 body_end，必须有人把它读掉。
		go drainRequestFrames(stream)
		return nil
	}
	reader, writer := io.Pipe()
	go pumpRequestBody(stream, writer)
	return reader
}

func pumpRequestBody(stream forwardStream, writer *io.PipeWriter) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// 宿主没发 body_end 就关了流：按截断处理，避免上游收到不完整的体还当成功。
				_ = writer.CloseWithError(errors.New("宿主在 body_end 之前关闭了转发流"))
				return
			}
			_ = writer.CloseWithError(err)
			return
		}
		switch payload := frame.GetFrame().(type) {
		case *pluginv1.ForwardRequest_BodyChunk:
			if len(payload.BodyChunk) == 0 {
				continue
			}
			if _, err := writer.Write(payload.BodyChunk); err != nil {
				// 上游或调用方已经不再读请求体，没必要继续接。
				return
			}
		case *pluginv1.ForwardRequest_BodyEnd:
			_ = writer.Close()
			return
		case *pluginv1.ForwardRequest_Start:
			_ = writer.CloseWithError(errors.New("转发流出现重复的 start 帧"))
			return
		}
	}
}

// drainRequestFrames 读空剩余请求帧，防止宿主卡在发送上。
func drainRequestFrames(stream forwardStream) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			return
		}
		if _, ok := frame.GetFrame().(*pluginv1.ForwardRequest_BodyEnd); ok {
			return
		}
	}
}

// streamResponseBody 边读边发，保证 SSE 这类长流的分块能立即到达宿主。
func streamResponseBody(stream forwardStream, body io.Reader) (int64, error) {
	buffer := make([]byte, responseChunkSize)
	var received int64
	for {
		read, readErr := body.Read(buffer)
		if read > 0 {
			received += int64(read)
			if sendErr := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{
				BodyChunk: append([]byte(nil), buffer[:read]...),
			}}); sendErr != nil {
				return received, &sendError{err: sendErr}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return received, nil
			}
			return received, readErr
		}
	}
}

// sendError 标记「失败发生在向宿主发送帧的路径上」，此时再发 error 帧也没有意义。
type sendError struct {
	err error
}

func (e *sendError) Error() string { return e.err.Error() }

func (e *sendError) Unwrap() error { return e.err }

func sendFailed(err error) bool {
	var failure *sendError
	return errors.As(err, &failure)
}

func sendErrorFrame(stream forwardStream, failure *transport.Error) error {
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Error{
		Error: &pluginv1.ForwardResponseError{
			Code:        failure.Code,
			Message:     failure.Message,
			RequestSent: failure.RequestSent,
		},
	}})
}

// headerFromFrame 保留宿主给出的键名大小写：宿主会精心构造上游请求头，
// 规范化反而会改变出站指纹。
func headerFromFrame(headers map[string]*pluginv1.HeaderValues) http.Header {
	out := make(http.Header, len(headers))
	for name, values := range headers {
		if values == nil {
			continue
		}
		out[name] = append([]string(nil), values.Values...)
	}
	return out
}

func headerToFrame(headers http.Header) map[string]*pluginv1.HeaderValues {
	out := make(map[string]*pluginv1.HeaderValues, len(headers))
	for name, values := range headers {
		out[name] = &pluginv1.HeaderValues{Values: append([]string(nil), values...)}
	}
	return out
}
