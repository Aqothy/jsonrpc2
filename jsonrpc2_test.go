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
	"path"
	"reflect"
	"strings"
	"testing"

	jsonrpc2 "github.com/Aqothy/jsonrpc2"
)

type binder struct {
	framer  jsonrpc2.Framer
	runTest func(*handler)
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

func (b binder) Bind(ctx context.Context, conn *jsonrpc2.Connection) jsonrpc2.ConnectionOptions {
	h := &handler{
		conn:    conn,
		waiters: make(chan map[string]chan struct{}, 1),
		calls:   make(map[string]*jsonrpc2.PendingCall),
	}
	h.waiters <- make(map[string]chan struct{})
	if b.runTest != nil {
		go b.runTest(h)
	}
	return jsonrpc2.ConnectionOptions{
		Framer:    b.framer,
		Preempter: h,
		Handler:   h,
	}
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
