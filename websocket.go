package jsonrpc2

import (
	"context"
	"fmt"
)

// WS doesnt need framing as its already message oriented
// Adapters for concrete websocket libraries should reject non-text websocket
// messages before returning data from ReadMessage.
type WebSocket interface {
	ReadMessage(ctx context.Context) ([]byte, error)
	WriteMessage(ctx context.Context, data []byte) error
	Close() error
}

type wsReader struct {
	ws WebSocket
}

type wsWriter struct {
	ws WebSocket
}

func NewWebSocketTransport(ws WebSocket) (Reader, Writer) {
	return &wsReader{ws: ws}, &wsWriter{ws: ws}
}

func (r *wsReader) Read(ctx context.Context) (Message, error) {
	data, err := r.ws.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	return DecodeMessage(data)
}

func (w *wsWriter) Write(ctx context.Context, msg Message) error {
	data, err := EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshaling websocket message: %w", err)
	}
	if err := w.ws.WriteMessage(ctx, data); err != nil {
		return fmt.Errorf("writing websocket message: %w", err)
	}
	return nil
}

type WebSocketConnectionOptions struct {
	Preempter           Preempter
	Handler             Handler
	OnDone              func()
	OnInternalError     func(error)
	OnNotificationError func(error)
}

func NewWebSocketConnection(ctx context.Context, ws WebSocket, handler Handler) *Connection {
	return NewWebSocketConnectionWithOptions(ctx, ws, WebSocketConnectionOptions{Handler: handler})
}

func NewWebSocketConnectionWithOptions(ctx context.Context, ws WebSocket, opts WebSocketConnectionOptions) *Connection {
	handler := opts.Handler
	if handler == nil {
		handler = defaultHandler{}
	}

	reader, writer := NewWebSocketTransport(ws)
	return NewConnection(ctx, ConnectionConfig{
		Reader:              reader,
		Writer:              writer,
		Closer:              ws,
		Preempter:           opts.Preempter,
		OnDone:              opts.OnDone,
		OnInternalError:     opts.OnInternalError,
		OnNotificationError: opts.OnNotificationError,
		Bind: func(*Connection) Handler {
			return handler
		},
	})
}
