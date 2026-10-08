// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	jsonrpc2 "github.com/Aqothy/jsonrpc2"
)

var callTests = []invoker{
	call{"no_args", nil, true},
	call{"one_string", "fish", "got:fish"},
	call{"one_number", 10, "got:10"},
	call{"join", []string{"a", "b", "c"}, "a/b/c"},
	sequence{"notify", []invoker{
		notify{"set", 3},
		notify{"add", 5},
		call{"get", nil, 8},
	}},
	sequence{"preempt", []invoker{
		async{"a", "wait", "a"},
		notify{"unblock", "a"},
		collect{"a", true, false},
	}},
	sequence{"basic cancel", []invoker{
		async{"b", "wait", "b"},
		cancel{"b"},
		collect{"b", nil, true},
	}},
	sequence{"queue", []invoker{
		async{"a", "wait", "a"},
		notify{"set", 1},
		notify{"add", 2},
		notify{"add", 3},
		notify{"add", 4},
		call{"peek", nil, 0}, // accumulator will not have any adds yet
		notify{"unblock", "a"},
		collect{"a", true, false},
		call{"get", nil, 10}, // accumulator now has all the adds
	}},
	sequence{"fork", []invoker{
		async{"a", "fork", "a"},
		notify{"set", 1},
		notify{"add", 2},
		notify{"add", 3},
		notify{"add", 4},
		call{"get", nil, 10}, // fork will not have blocked the adds
		notify{"unblock", "a"},
		collect{"a", true, false},
	}},
	sequence{"concurrent", []invoker{
		async{"a", "fork", "a"},
		notify{"unblock", "a"},
		async{"b", "fork", "b"},
		notify{"unblock", "b"},
		collect{"a", true, false},
		collect{"b", true, false},
	}},
}

type handler struct {
	conn        *jsonrpc2.Connection
	accumulator int
	waiters     chan map[string]chan struct{}
	calls       map[string]*jsonrpc2.PendingCall
}

type invoker interface {
	Name() string
	Invoke(t *testing.T, ctx context.Context, h *handler)
}

type notify struct {
	method string
	params any
}

type call struct {
	method string
	params any
	expect any
}

type async struct {
	name   string
	method string
	params any
}

type collect struct {
	name   string
	expect any
	fails  bool
}

type cancel struct {
	name string
}

type sequence struct {
	name  string
	tests []invoker
}

type echo call

type cancelParams struct{ ID int64 }

func (test notify) Name() string { return test.method }
func (test notify) Invoke(t *testing.T, ctx context.Context, h *handler) {
	if err := h.conn.Notify(ctx, test.method, test.params); err != nil {
		t.Fatalf("%v:Notify failed: %v", test.method, err)
	}
}

func (test call) Name() string { return test.method }
func (test call) Invoke(t *testing.T, ctx context.Context, h *handler) {
	results := newResults(test.expect)
	if err := h.conn.Call(ctx, test.method, test.params).Await(ctx, results); err != nil {
		t.Fatalf("%v:Call failed: %v", test.method, err)
	}
	verifyResults(t, test.method, results, test.expect)
}

func (test echo) Invoke(t *testing.T, ctx context.Context, h *handler) {
	results := newResults(test.expect)
	if err := h.conn.Call(ctx, "echo", []any{test.method, test.params}).Await(ctx, results); err != nil {
		t.Fatalf("%v:Echo failed: %v", test.method, err)
	}
	verifyResults(t, test.method, results, test.expect)
}

func (test async) Name() string { return test.name }
func (test async) Invoke(t *testing.T, ctx context.Context, h *handler) {
	h.calls[test.name] = h.conn.Call(ctx, test.method, test.params)
}

func (test collect) Name() string { return test.name }
func (test collect) Invoke(t *testing.T, ctx context.Context, h *handler) {
	o := h.calls[test.name]
	results := newResults(test.expect)
	err := o.Await(ctx, results)
	switch {
	case test.fails && err == nil:
		t.Fatalf("%v:Collect was supposed to fail", test.name)
	case !test.fails && err != nil:
		t.Fatalf("%v:Collect failed: %v", test.name, err)
	}
	verifyResults(t, test.name, results, test.expect)
}

func (test cancel) Name() string { return test.name }
func (test cancel) Invoke(t *testing.T, ctx context.Context, h *handler) {
	o := h.calls[test.name]
	if err := h.conn.Notify(ctx, "cancel", &cancelParams{o.ID().Raw().(int64)}); err != nil {
		t.Fatalf("%v:Collect failed: %v", test.name, err)
	}
}

func (test sequence) Name() string { return test.name }
func (test sequence) Invoke(t *testing.T, ctx context.Context, h *handler) {
	for _, child := range test.tests {
		child.Invoke(t, ctx, h)
	}
}

// newResults makes a new empty copy of the expected type to put the results into
func newResults(expect any) any {
	switch e := expect.(type) {
	case []any:
		var r []any
		for _, v := range e {
			r = append(r, reflect.New(reflect.TypeOf(v)).Interface())
		}
		return r
	case nil:
		return nil
	default:
		return reflect.New(reflect.TypeOf(expect)).Interface()
	}
}

// verifyResults compares the results to the expected values
func verifyResults(t *testing.T, method string, results any, expect any) {
	if expect == nil {
		if results != nil {
			t.Errorf("%v:Got results %+v where none expected", method, expect)
		}
		return
	}
	val := reflect.Indirect(reflect.ValueOf(results)).Interface()
	if !reflect.DeepEqual(val, expect) {
		t.Errorf("%v:Results are incorrect, got %+v expect %+v", method, val, expect)
	}
}

func TestConnectionHeader(t *testing.T) {
	testConnection(t, jsonrpc2.NewHeaderFramer())
}

func TestConnectionNd(t *testing.T) {
	testConnection(t, jsonrpc2.NewNdFramer())
}

// testConnection runs the callTests matrix over a pair of connections joined
// by an in-memory pipe. Both ends serve the full handler, so nested calls
// (echo) exercise both directions.
func testConnection(t *testing.T, framer jsonrpc2.Framer) {
	ctx := context.Background()
	for _, test := range callTests {
		t.Run(test.Name(), func(t *testing.T) {
			clientPipe, serverPipe := net.Pipe()
			server := newTestConnection(ctx, serverPipe, framer, nil)
			client := newTestConnection(ctx, clientPipe, framer, func(h *handler) {
				defer h.conn.Close()
				test.Invoke(t, ctx, h)
				if call, ok := test.(call); ok {
					// also run all simple call tests in echo mode
					echo(call).Invoke(t, ctx, h)
				}
			})
			client.Wait()
			server.Close()
		})
	}
}

// newTestConnection binds a handler to one end of a pipe. If runTest is
// non-nil it is started once the connection is ready.
func newTestConnection(ctx context.Context, rwc io.ReadWriteCloser, framer jsonrpc2.Framer, runTest func(*handler)) *jsonrpc2.Connection {
	reader, writer := framer.Frame(rwc)
	h := &handler{
		waiters: make(chan map[string]chan struct{}, 1),
		calls:   make(map[string]*jsonrpc2.PendingCall),
	}
	h.waiters <- make(map[string]chan struct{})
	return jsonrpc2.NewConnection(ctx, jsonrpc2.ConnectionConfig{
		Reader:    reader,
		Writer:    writer,
		Closer:    rwc,
		Preempter: h,
		Bind: func(conn *jsonrpc2.Connection) jsonrpc2.Handler {
			h.conn = conn
			if runTest != nil {
				go runTest(h)
			}
			return h
		},
	})
}

func (h *handler) waiter(name string) chan struct{} {
	waiters := <-h.waiters
	defer func() { h.waiters <- waiters }()
	waiter, found := waiters[name]
	if !found {
		waiter = make(chan struct{})
		waiters[name] = waiter
	}
	return waiter
}

func (h *handler) Preempt(ctx context.Context, req *jsonrpc2.Request) (any, error) {
	switch req.Method {
	case "unblock":
		var name string
		if err := json.Unmarshal(req.Params, &name); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		close(h.waiter(name))
		return nil, nil
	case "peek":
		if len(req.Params) > 0 {
			return nil, fmt.Errorf("%w: expected no params", jsonrpc2.ErrInvalidParams)
		}
		return h.accumulator, nil
	case "cancel":
		var params cancelParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		h.conn.Cancel(jsonrpc2.Int64ID(params.ID))
		return nil, nil
	default:
		return nil, jsonrpc2.ErrNotHandled
	}
}

func TestUnexpectedResponseReportsInternalError(t *testing.T) {
	reported := make(chan error, 1)
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &queuedReader{msgs: []jsonrpc2.Message{
			&jsonrpc2.Response{ID: jsonrpc2.Int64ID(99)},
		}},
		Writer: discardWriter{},
		Closer: noopCloser{},
		Bind: func(*jsonrpc2.Connection) jsonrpc2.Handler {
			return jsonrpc2.HandlerFunc(func(context.Context, *jsonrpc2.Request) (any, error) {
				t.Fatal("unexpected request")
				return nil, nil
			})
		},
		OnInternalError: func(err error) { reported <- err },
	})
	if err := conn.Wait(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "unknown request ID") {
			t.Fatalf("reported error = %v, want unknown request ID", err)
		}
	default:
		t.Fatal("expected internal error report")
	}
}

func TestNotificationErrorReportsNotificationError(t *testing.T) {
	notifyErr := errors.New("boom")
	reported := make(chan error, 1)
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &queuedReader{msgs: []jsonrpc2.Message{
			&jsonrpc2.Request{Method: "notify"},
		}},
		Writer: discardWriter{},
		Closer: noopCloser{},
		Bind: func(*jsonrpc2.Connection) jsonrpc2.Handler {
			return jsonrpc2.HandlerFunc(func(context.Context, *jsonrpc2.Request) (any, error) {
				return nil, notifyErr
			})
		},
		OnNotificationError: func(err error) { reported <- err },
	})
	if err := conn.Wait(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "notification failed") || !strings.Contains(err.Error(), notifyErr.Error()) {
			t.Fatalf("reported error = %v, want notification failure wrapping %v", err, notifyErr)
		}
	default:
		t.Fatal("expected notification error report")
	}
}

type queuedReader struct {
	msgs []jsonrpc2.Message
}

func (r *queuedReader) Read(context.Context) (jsonrpc2.Message, error) {
	if len(r.msgs) == 0 {
		return nil, io.EOF
	}
	msg := r.msgs[0]
	r.msgs = r.msgs[1:]
	return msg, nil
}

type discardWriter struct{}

func (discardWriter) Write(context.Context, jsonrpc2.Message) error { return nil }

type noopCloser struct{}

func (noopCloser) Close() error { return nil }

func (h *handler) Handle(ctx context.Context, req *jsonrpc2.Request) (any, error) {
	switch req.Method {
	case "no_args":
		if len(req.Params) > 0 {
			return nil, fmt.Errorf("%w: expected no params", jsonrpc2.ErrInvalidParams)
		}
		return true, nil
	case "one_string":
		var v string
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		return "got:" + v, nil
	case "one_number":
		var v int
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		return fmt.Sprintf("got:%d", v), nil
	case "set":
		var v int
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		h.accumulator = v
		return nil, nil
	case "add":
		var v int
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		h.accumulator += v
		return nil, nil
	case "get":
		if len(req.Params) > 0 {
			return nil, fmt.Errorf("%w: expected no params", jsonrpc2.ErrInvalidParams)
		}
		return h.accumulator, nil
	case "join":
		var v []string
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		return path.Join(v...), nil
	case "echo":
		var v []any
		if err := json.Unmarshal(req.Params, &v); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		var result any
		err := h.conn.Call(ctx, v[0].(string), v[1]).Await(ctx, &result)
		return result, err
	case "wait":
		var name string
		if err := json.Unmarshal(req.Params, &name); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		select {
		case <-h.waiter(name):
			return true, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case "fork":
		var name string
		if err := json.Unmarshal(req.Params, &name); err != nil {
			return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrParse, err)
		}
		waitFor := h.waiter(name)
		go func() {
			select {
			case <-waitFor:
				h.conn.Respond(req.ID, true, nil)
			case <-ctx.Done():
				h.conn.Respond(req.ID, nil, ctx.Err())
			}
		}()
		return nil, jsonrpc2.ErrAsyncResponse
	default:
		return nil, jsonrpc2.ErrNotHandled
	}
}

type streamReadResult struct {
	msg jsonrpc2.Message
	err error
}

type streamChannelReader struct {
	ch <-chan streamReadResult
}

func (r *streamChannelReader) Read(context.Context) (jsonrpc2.Message, error) {
	v := <-r.ch
	return v.msg, v.err
}

type captureTestWriter struct {
	ch chan jsonrpc2.Message
}

func newCaptureTestWriter() *captureTestWriter {
	return &captureTestWriter{ch: make(chan jsonrpc2.Message, 8)}
}

func (w *captureTestWriter) Write(ctx context.Context, msg jsonrpc2.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.ch <- msg:
		return nil
	}
}

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func TestWaitReturnsReadError(t *testing.T) {
	readErr := errors.New("read failed")
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &streamChannelReader{ch: closedStreamReadResults(streamReadResult{err: readErr})},
		Writer: discardWriter{},
		Closer: closeFunc(func() error { return nil }),
	})

	if err := conn.Wait(); !errors.Is(err, readErr) {
		t.Fatalf("Wait() = %v, want %v", err, readErr)
	}
}

func TestMalformedHandlerResultSendsInternalErrorResponse(t *testing.T) {
	writer := newCaptureTestWriter()
	reported := make(chan error, 1)
	reads := make(chan streamReadResult, 2)
	reads <- streamReadResult{msg: &jsonrpc2.Request{ID: jsonrpc2.Int64ID(1), Method: "bad"}}
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader:          &streamChannelReader{ch: reads},
		Writer:          writer,
		Closer:          closeFunc(func() error { return nil }),
		OnInternalError: func(err error) { reported <- err },
		Bind: func(*jsonrpc2.Connection) jsonrpc2.Handler {
			return jsonrpc2.HandlerFunc(func(context.Context, *jsonrpc2.Request) (any, error) {
				return func() {}, nil
			})
		},
	})
	msg := <-writer.ch
	reads <- streamReadResult{err: io.EOF}
	if err := conn.Wait(); err != nil {
		t.Fatal(err)
	}
	resp, ok := msg.(*jsonrpc2.Response)
	if !ok {
		t.Fatalf("wrote %T, want *Response", msg)
	}
	if !errors.Is(resp.Error, jsonrpc2.ErrInternal) {
		t.Fatalf("response error = %v, want ErrInternal", resp.Error)
	}
	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "malformed result") {
			t.Fatalf("reported error = %v, want malformed result", err)
		}
	default:
		t.Fatal("expected internal error report")
	}
}

func TestRetireAllowsCloseAfterCanceledAwait(t *testing.T) {
	readerClosed := make(chan struct{})
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &blockingTestReader{closed: readerClosed},
		Writer: discardWriter{},
		Closer: closeFunc(func() error {
			close(readerClosed)
			return nil
		}),
	})

	call := conn.Call(context.Background(), "slow", nil)
	conn.Retire(call, context.Canceled)
	if err := call.Await(context.Background(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Await() = %v, want context.Canceled", err)
	}
	assertCompletes(t, conn.Close)
}

func TestCloseStillPreemptsNotifications(t *testing.T) {
	reads := make(chan streamReadResult, 2)
	preempted := make(chan struct{}, 1)
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &streamChannelReader{ch: reads},
		Writer: discardWriter{},
		Closer: closeFunc(func() error { return nil }),
		Preempter: jsonrpc2.PreempterFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			if req.Method == "cancel" {
				preempted <- struct{}{}
				return nil, nil
			}
			return nil, jsonrpc2.ErrNotHandled
		}),
		OnNotificationError: func(error) {},
	})

	done := make(chan error, 1)
	go func() { done <- conn.Close() }()
	reads <- streamReadResult{msg: &jsonrpc2.Request{Method: "cancel"}}
	reads <- streamReadResult{err: io.EOF}

	select {
	case <-preempted:
	case <-time.After(time.Second):
		t.Fatal("notification was not preempted during close")
	}
	if err := <-done; err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestNullResultResponseRoundTrip(t *testing.T) {
	msg, err := jsonrpc2.NewResponse(jsonrpc2.Int64ID(1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := jsonrpc2.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"result":null`) {
		t.Fatalf("encoded response = %s, want result:null", data)
	}
	decoded, err := jsonrpc2.DecodeMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := decoded.(*jsonrpc2.Response)
	if !ok {
		t.Fatalf("decoded %T, want *Response", decoded)
	}
	if string(resp.Result) != "null" || resp.Error != nil {
		t.Fatalf("decoded response = %#v, want null result and no error", resp)
	}
}

func TestDecodeNullRequestIDAsMissing(t *testing.T) {
	msg, err := jsonrpc2.DecodeMessage([]byte(`{"jsonrpc":"2.0","id":null,"method":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	req, ok := msg.(*jsonrpc2.Request)
	if !ok {
		t.Fatalf("decoded %T, want *Request", msg)
	}
	if req.IsCall() {
		t.Fatal("null request ID should be treated as missing")
	}
}

// A response with a null id (typically a peer reporting our malformed
// message) decodes as an unroutable response, never as a request error that
// would itself be answered.
func TestDecodeNullResponseIDIsUnroutableResponse(t *testing.T) {
	msg, err := jsonrpc2.DecodeMessage([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := msg.(*jsonrpc2.Response)
	if !ok || resp.ID.IsValid() || !errors.Is(resp.Error, jsonrpc2.ErrParse) {
		t.Fatalf("decoded %#v, want response with null id and parse error", msg)
	}
}

func TestMissingIDIsNotification(t *testing.T) {
	msg, err := jsonrpc2.DecodeMessage([]byte(`{"jsonrpc":"2.0","method":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	req, ok := msg.(*jsonrpc2.Request)
	if !ok {
		t.Fatalf("decoded %T, want *Request", msg)
	}
	if req.IsCall() {
		t.Fatal("missing request ID should be a notification")
	}
	data, err := jsonrpc2.EncodeMessage(req)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"id"`) {
		t.Fatalf("encoded notification = %s, want no id member", data)
	}
}

func TestHandlerNilResultSendsNullResult(t *testing.T) {
	writer := newCaptureTestWriter()
	reads := make(chan streamReadResult, 2)
	reads <- streamReadResult{msg: &jsonrpc2.Request{ID: jsonrpc2.Int64ID(1), Method: "null"}}
	conn := jsonrpc2.NewConnection(context.Background(), jsonrpc2.ConnectionConfig{
		Reader: &streamChannelReader{ch: reads},
		Writer: writer,
		Closer: closeFunc(func() error { return nil }),
		Bind: func(*jsonrpc2.Connection) jsonrpc2.Handler {
			return jsonrpc2.HandlerFunc(func(context.Context, *jsonrpc2.Request) (any, error) {
				return nil, nil
			})
		},
	})
	msg := <-writer.ch
	reads <- streamReadResult{err: io.EOF}
	if err := conn.Wait(); err != nil {
		t.Fatal(err)
	}
	resp, ok := msg.(*jsonrpc2.Response)
	if !ok {
		t.Fatal("wrote non-response")
	}
	if string(resp.Result) != "null" || resp.Error != nil {
		t.Fatalf("response = %#v, want result:null", resp)
	}
}

func TestResponseMarshalFillsNullResult(t *testing.T) {
	data, err := jsonrpc2.EncodeMessage(&jsonrpc2.Response{ID: jsonrpc2.Int64ID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"result":null`) {
		t.Fatalf("encoded response = %s, want result:null", data)
	}
}

func TestResponseMarshalErrorOmitsResult(t *testing.T) {
	data, err := jsonrpc2.EncodeMessage(&jsonrpc2.Response{
		ID:     jsonrpc2.Int64ID(1),
		Result: json.RawMessage(`"ignored"`),
		Error:  errors.New("boom"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"result"`) {
		t.Fatalf("encoded response = %s, want error response without result", data)
	}
}

func TestGenericErrorEncodesUnknownCode(t *testing.T) {
	msg, err := jsonrpc2.NewResponse(jsonrpc2.Int64ID(1), nil, errors.New("boom"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := jsonrpc2.EncodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := jsonrpc2.DecodeMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := decoded.(*jsonrpc2.Response)
	if !ok {
		t.Fatalf("decoded %T, want *Response", decoded)
	}
	if !errors.Is(resp.Error, jsonrpc2.ErrUnknown) {
		t.Fatalf("response error = %v, want ErrUnknown", resp.Error)
	}
}

func TestMakeID(t *testing.T) {
	id, err := jsonrpc2.MakeID(nil)
	if err != nil {
		t.Fatalf("MakeID(nil) failed: %v", err)
	}
	if id.IsValid() {
		t.Fatalf("MakeID(nil) = %s valid:true, want missing ID", id.String())
	}

	tests := []struct {
		in   any
		want string
	}{
		{float64(3), "3"},
		{int64(4), "4"},
		{"x", `"x"`},
	}
	for _, test := range tests {
		id, err := jsonrpc2.MakeID(test.in)
		if err != nil {
			t.Fatalf("MakeID(%#v) failed: %v", test.in, err)
		}
		if !id.IsValid() || id.String() != test.want {
			t.Fatalf("MakeID(%#v) = %s valid:%v, want %s valid:true", test.in, id.String(), id.IsValid(), test.want)
		}
	}
	if _, err := jsonrpc2.MakeID(float64(1.5)); !errors.Is(err, jsonrpc2.ErrParse) {
		t.Fatalf("MakeID(1.5) = %v, want ErrParse", err)
	}
}

func TestDecodeRejectsFractionalID(t *testing.T) {
	_, err := jsonrpc2.DecodeMessage([]byte(`{"jsonrpc":"2.0","id":1.5,"method":"x"}`))
	if !errors.Is(err, jsonrpc2.ErrParse) {
		t.Fatalf("DecodeMessage fractional ID error = %v, want ErrParse", err)
	}
}

type blockingTestReader struct {
	closed <-chan struct{}
}

func (r *blockingTestReader) Read(context.Context) (jsonrpc2.Message, error) {
	<-r.closed
	return nil, io.EOF
}

func closedStreamReadResults(results ...streamReadResult) <-chan streamReadResult {
	ch := make(chan streamReadResult, len(results))
	for _, result := range results {
		ch <- result
	}
	return ch
}

func assertCompletes(t *testing.T, f func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation did not complete")
	}
}
