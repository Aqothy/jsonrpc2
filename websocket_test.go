package jsonrpc2

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type mockWebSocket struct {
	readc    chan []byte
	writec   chan []byte
	closec   chan struct{}
	closeErr error
}

func newMockWebSocket() *mockWebSocket {
	return &mockWebSocket{
		readc:  make(chan []byte, 1),
		writec: make(chan []byte, 1),
		closec: make(chan struct{}),
	}
}

func (m *mockWebSocket) ReadMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.closec:
		return nil, errors.New("websocket closed")
	case data := <-m.readc:
		return data, nil
	}
}

func (m *mockWebSocket) WriteMessage(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closec:
		return errors.New("websocket closed")
	case m.writec <- data:
		return nil
	}
}

func (m *mockWebSocket) Close() error {
	select {
	case <-m.closec:
	default:
		close(m.closec)
	}
	return m.closeErr
}

func TestWebSocketReaderWriter(t *testing.T) {
	ctx := context.Background()
	ws := newMockWebSocket()
	r, w := NewWebSocketTransport(ws)

	msg, _ := NewNotification("test", nil)
	if err := w.Write(ctx, msg); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	ws.readc <- <-ws.writec
	readMsg, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(msg, readMsg) {
		t.Errorf("expected %+v, got %+v", msg, readMsg)
	}
}

func TestWebSocketReaderRejectsBatch(t *testing.T) {
	ctx := context.Background()
	ws := newMockWebSocket()
	r, _ := NewWebSocketTransport(ws)

	ws.readc <- []byte(`[{"jsonrpc":"2.0","method":"one"},{"jsonrpc":"2.0","method":"two"}]`)
	_, err := r.Read(ctx)
	if err == nil {
		t.Fatal("expected batch to be rejected")
	}
}

func TestWebSocketConnectionUsesCloser(t *testing.T) {
	ctx := context.Background()
	ws := newMockWebSocket()
	done := make(chan struct{}, 1)

	conn := NewWebSocketConnectionWithOptions(ctx, ws, WebSocketConnectionOptions{
		OnDone:          func() { done <- struct{}{} },
		OnInternalError: func(error) {},
	})
	if err := conn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case <-done:
	default:
		t.Fatal("OnDone was not called")
	}
}
