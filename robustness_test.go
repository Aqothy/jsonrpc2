package jsonrpc2_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Aqothy/jsonrpc2"
)

// rawPeer drives one end of a newline-delimited connection by hand.
type rawPeer struct {
	conn net.Conn
	in   *bufio.Reader
}

func newRawPeer(t *testing.T, handler jsonrpc2.Handler) (*rawPeer, *jsonrpc2.Connection) {
	t.Helper()
	a, b := net.Pipe()
	conn := jsonrpc2.NewStreamConnection(context.Background(), a, jsonrpc2.ConnectionOptions{
		Framer:          jsonrpc2.NewNdFramer(),
		Handler:         handler,
		OnInternalError: func(error) {},
	})
	t.Cleanup(func() {
		_ = b.Close()
		_ = conn.Close()
	})
	return &rawPeer{conn: b, in: bufio.NewReader(b)}, conn
}

func (p *rawPeer) send(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(p.conn, line+"\n"); err != nil {
		t.Fatal(err)
	}
}

func (p *rawPeer) receive(t *testing.T) map[string]any {
	t.Helper()
	_ = p.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := p.in.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		t.Fatalf("peer received invalid JSON %q: %v", line, err)
	}
	return msg
}

func errorCode(t *testing.T, msg map[string]any) float64 {
	t.Helper()
	e, ok := msg["error"].(map[string]any)
	if !ok {
		t.Fatalf("message %v has no error", msg)
	}
	return e["code"].(float64)
}

func TestMalformedInputIsAnsweredAndConnectionSurvives(t *testing.T) {
	peer, _ := newRawPeer(t, jsonrpc2.HandlerFunc(func(context.Context, *jsonrpc2.Request) (any, error) {
		return "ok", nil
	}))

	peer.send(t, "")
	peer.send(t, "{not json")
	if msg := peer.receive(t); errorCode(t, msg) != -32700 || msg["id"] != nil {
		t.Fatalf("parse error response = %v, want code -32700 with id null", msg)
	}
	peer.send(t, `[{"jsonrpc":"2.0","method":"a"}]`)
	if msg := peer.receive(t); errorCode(t, msg) != -32600 || msg["id"] != nil {
		t.Fatalf("batch response = %v, want code -32600 with id null", msg)
	}
	peer.send(t, `{"jsonrpc":"2.0"}`)
	if msg := peer.receive(t); errorCode(t, msg) != -32600 {
		t.Fatalf("empty message response = %v, want code -32600", msg)
	}
	// A response with a null id is never answered (that could loop between
	// peers); the next reply must be for the following request.
	peer.send(t, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad"}}`)
	peer.send(t, `{"id":7,"method":"still_alive"}`)
	if msg := peer.receive(t); msg["id"] != float64(7) || msg["result"] != "ok" {
		t.Fatalf("response = %v, want result for id 7", msg)
	}
}

func TestMalformedResponseFailsPendingCall(t *testing.T) {
	peer, conn := newRawPeer(t, nil)
	result := make(chan error, 1)
	go func() { result <- conn.Call(context.Background(), "x", nil).Await(context.Background(), nil) }()
	if req := peer.receive(t); req["id"] != float64(1) {
		t.Fatalf("call = %v, want id 1", req)
	}
	peer.send(t, `{"jsonrpc":"2.0","id":1}`)
	if err := <-result; !errors.Is(err, jsonrpc2.ErrInvalidRequest) {
		t.Fatalf("Await() = %v, want ErrInvalidRequest", err)
	}
}

func TestHandlersRunInOrderUntilAsync(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondDone := make(chan struct{})
	peer, _ := newRawPeer(t, jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
		switch req.Method {
		case "first":
			close(firstStarted)
			<-releaseFirst
			jsonrpc2.Async(ctx)
			<-secondDone
			return "first", nil
		case "second":
			close(secondDone)
			return "second", nil
		}
		return nil, jsonrpc2.ErrNotHandled
	}))

	peer.send(t, `{"jsonrpc":"2.0","id":1,"method":"first"}`)
	peer.send(t, `{"jsonrpc":"2.0","id":2,"method":"second"}`)
	<-firstStarted
	select {
	case <-secondDone:
		t.Fatal("second handler ran before first called Async")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if msg := peer.receive(t); msg["result"] != "second" {
		t.Fatalf("first response = %v, want the second call to finish first", msg)
	}
	if msg := peer.receive(t); msg["result"] != "first" {
		t.Fatalf("second response = %v, want first", msg)
	}
}

func TestPeerDisconnectCancelsParkedHandler(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan error, 1)
	peer, conn := newRawPeer(t, jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
		jsonrpc2.Async(ctx)
		close(started)
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		return nil, ctx.Err()
	}))
	peer.send(t, `{"jsonrpc":"2.0","id":1,"method":"park"}`)
	<-started
	_ = peer.conn.Close()

	select {
	case cause := <-cancelled:
		if !errors.Is(cause, jsonrpc2.ErrServerClosing) {
			t.Fatalf("cause = %v, want ErrServerClosing", cause)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled when the peer went away")
	}
	assertCompletes(t, func() error { _ = conn.Wait(); return nil })
}

func TestCloseCancelsHandlersAndFailsPendingCalls(t *testing.T) {
	started := make(chan struct{})
	peer, conn := newRawPeer(t, jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
		jsonrpc2.Async(ctx)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	go func() { _, _ = io.Copy(io.Discard, peer.in) }()
	peer.send(t, `{"jsonrpc":"2.0","id":1,"method":"park"}`)
	<-started
	call := conn.Call(context.Background(), "never_answered", nil)

	assertCompletes(t, conn.Close)
	if err := call.Await(context.Background(), nil); !errors.Is(err, jsonrpc2.ErrClientClosing) {
		t.Fatalf("Await() = %v, want ErrClientClosing", err)
	}
}

func TestFramersEnforceMessageSizeLimit(t *testing.T) {
	big := `{"jsonrpc":"2.0","method":"` + strings.Repeat("x", 64) + `"}`
	tests := []struct {
		name   string
		framer jsonrpc2.Framer
		data   string
	}{
		{"nd", jsonrpc2.NdFramer{MaxMessageBytes: 32}, big + "\n"},
		{"header", jsonrpc2.HeaderFramer{MaxMessageBytes: 32}, "Content-Length: 90\r\n\r\n" + big},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, _ := test.framer.Frame(rwc{Reader: strings.NewReader(test.data)})
			if _, err := r.Read(context.Background()); !errors.Is(err, jsonrpc2.ErrMessageTooLarge) {
				t.Fatalf("Read() = %v, want ErrMessageTooLarge", err)
			}
		})
	}

	r, _ := jsonrpc2.NdFramer{MaxMessageBytes: len(big)}.Frame(rwc{Reader: strings.NewReader(big + "\r\n")})
	if _, err := r.Read(context.Background()); err != nil {
		t.Fatalf("message at the limit: %v", err)
	}
}

type rwc struct{ io.Reader }

func (rwc) Write(p []byte) (int, error) { return len(p), nil }
func (rwc) Close() error                { return nil }

// A call's then runs as its response is read, before the message after it is
// handled, however the goroutine awaiting the call is scheduled.
func TestCallThenRunsBeforeTheNextMessage(t *testing.T) {
	a, b := net.Pipe()
	events := make(chan string, 2)
	conn := jsonrpc2.NewStreamConnection(context.Background(), a, jsonrpc2.ConnectionOptions{
		Framer: jsonrpc2.NewNdFramer(),
		Preempter: jsonrpc2.PreempterFunc(func(_ context.Context, req *jsonrpc2.Request) (any, error) {
			events <- req.Method
			return nil, nil
		}),
		OnInternalError: func(error) {},
	})
	t.Cleanup(func() {
		_ = b.Close()
		_ = conn.Close()
	})
	peer := &rawPeer{conn: b, in: bufio.NewReader(b)}

	awaited := make(chan error, 1)
	go func() {
		call := conn.CallThen(context.Background(), "start", nil, func(response *jsonrpc2.Response) {
			events <- "then " + string(response.Result)
		})
		awaited <- call.Await(context.Background(), nil)
	}()
	if req := peer.receive(t); req["method"] != "start" {
		t.Fatalf("call = %v", req)
	}
	// The response and the notification it makes possible arrive together.
	peer.send(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`+"\n"+`{"jsonrpc":"2.0","method":"started"}`)
	for _, want := range []string{`then "ok"`, "started"} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event = %q, want %q: then runs before the next message", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no event, want %q", want)
		}
	}
	if err := <-awaited; err != nil {
		t.Fatal(err)
	}
}
