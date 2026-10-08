// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// option for new connections
type ConnectionOptions struct {
	// Framer allows control over the message framing and encoding.
	// If nil, HeaderFramer will be used.
	Framer Framer
	// Preempter allows registration of a pre-queue message handler.
	// If nil, no messages will be preempted.
	Preempter Preempter
	// Handler is used as the queued message handler for inbound messages.
	// If nil, all responses will be ErrNotHandled.
	Handler Handler
	// OnInternalError, if non-nil, is called with any internal errors that occur
	// while serving the connection, such as protocol errors or invariant
	// violations. (If nil, internal errors result in panics.)
	OnInternalError func(error)
	// OnNotificationError, if non-nil, is called with errors returned while
	// handling notifications. Notifications cannot receive wire responses, so
	// these errors are otherwise dropped.
	OnNotificationError func(error)
	OnDone              func()
}

// connection config for bidirectional jsonrpc2 connection
type ConnectionConfig struct {
	Reader              Reader
	Writer              Writer
	Closer              io.Closer
	Preempter           Preempter
	Bind                func(*Connection) Handler
	OnDone              func()
	OnInternalError     func(error)
	OnNotificationError func(error)
}

// NewConnection creates a connection from fully constructed transport pieces.
func NewConnection(ctx context.Context, cfg ConnectionConfig) *Connection {
	ctx = context.WithoutCancel(ctx)

	c := &Connection{
		state:               inFlightState{closer: cfg.Closer},
		done:                make(chan struct{}),
		writer:              make(chan Writer, 1),
		onDone:              cfg.OnDone,
		onInternalError:     cfg.OnInternalError,
		onNotificationError: cfg.OnNotificationError,
	}

	if cfg.Bind != nil {
		c.handler = cfg.Bind(c)
	}
	if c.handler == nil {
		c.handler = defaultHandler{}
	}
	c.writer <- cfg.Writer
	c.start(ctx, cfg.Reader, cfg.Preempter)

	return c
}

// NewStreamConnection creates a connection over rwc using opts.Framer.
// If opts.Framer is nil, NewHeaderFramer is used.
func NewStreamConnection(ctx context.Context, rwc io.ReadWriteCloser, opts ConnectionOptions) *Connection {
	framer := opts.Framer
	if framer == nil {
		framer = NewHeaderFramer()
	}
	handler := opts.Handler
	if handler == nil {
		handler = defaultHandler{}
	}
	reader, writer := framer.Frame(rwc)
	return NewConnection(ctx, ConnectionConfig{
		Reader:              reader,
		Writer:              writer,
		Closer:              rwc,
		Preempter:           opts.Preempter,
		OnDone:              opts.OnDone,
		OnInternalError:     opts.OnInternalError,
		OnNotificationError: opts.OnNotificationError,
		Bind: func(*Connection) Handler {
			return handler
		},
	})
}

// Connection manages the jsonrpc2 protocol, connecting responses back to their
// calls.
// Connection is bidirectional; it does not have a designated server or client
// end.
type Connection struct {
	seq atomic.Int64 // must only be accessed using atomic operations

	stateMu sync.Mutex
	state   inFlightState // accessed only in updateInFlight
	done    chan struct{} // closed (under stateMu) when state.closed is true and all goroutines have completed

	writer chan Writer // 1-buffered; stores the writer when not in use

	handler Handler

	onInternalError     func(error)
	onNotificationError func(error)
	onDone              func()
}

// Invokes target method and returns PendingCall which can be used to await the
// response
// You don't have to wait for the response, it can be ignored if not needed
// If sending the call failed then the response will be ready and have the error in it
func (c *Connection) Call(ctx context.Context, method string, params any) *PendingCall {
	id := Int64ID(c.seq.Add(1))

	ac := &PendingCall{
		id:    id,
		ready: make(chan struct{}),
	}

	// When this method returns, either ac is retired, or the request has been
	// written successfully and the call is awaiting a response (to be provided by
	// the readIncoming goroutine).

	call, err := NewCall(ac.id, method, params)
	if err != nil {
		ac.retire(&Response{ID: id, Error: fmt.Errorf("marshaling call parameters: %w", err)})
		return ac
	}

	c.updateInFlight(func(s *inFlightState) {
		err = s.shouldShutDown(ErrClientClosing)
		if err != nil {
			return
		}

		if s.outgoingCalls == nil {
			s.outgoingCalls = make(map[ID]*PendingCall)
		}

		s.outgoingCalls[ac.id] = ac
	})

	if err != nil {
		ac.retire(&Response{ID: id, Error: err})
		return ac
	}

	if err := c.write(ctx, call); err != nil {
		c.Retire(ac, err)
	}

	return ac
}

// Retire stops tracking ac and completes it locally with err.
//
// Retire is useful when the caller has abandoned a call, usually because its
// Await context was canceled, and does not want connection shutdown to wait for
// a response that is no longer useful. If the peer later sends the real
// response, the connection will treat it as an unexpected response.
func (c *Connection) Retire(ac *PendingCall, err error) {
	if ac == nil {
		return
	}
	c.updateInFlight(func(s *inFlightState) {
		if s.outgoingCalls[ac.id] == ac {
			delete(s.outgoingCalls, ac.id)
			ac.retire(&Response{ID: ac.id, Error: err})
		}
	})
}

// Responds to an incoming call.
func (c *Connection) Respond(id ID, result any, err error) error {
	var req *incomingRequest
	c.updateInFlight(func(s *inFlightState) {
		req = s.incomingByID[id]
	})
	if req == nil {
		return c.internalErrorf("Request not found for ID %v", id)
	}

	if errors.Is(err, ErrAsyncResponse) {
		err = c.internalErrorf("Respond called with ErrAsyncResponse for %q", req.Method)
	}

	return c.processResult("Respond", req, result, err)
}

func (c *Connection) acceptRequest(ctx context.Context, msg *Request, preempter Preempter) {
	reqCtx, cancel := context.WithCancelCause(ctx)

	req := &incomingRequest{
		Request: msg,
		ctx:     reqCtx,
		cancel:  cancel,
	}

	var err error
	c.updateInFlight(func(s *inFlightState) {
		s.incoming++

		if req.IsCall() {
			if s.incomingByID[req.ID] != nil {
				err = fmt.Errorf("%w: request ID %v already in use", ErrInvalidRequest, req.ID)
				req.ID = ID{} // Don't misattribute this error to the existing request.
				return
			}

			if s.incomingByID == nil {
				s.incomingByID = make(map[ID]*incomingRequest)
			}
			s.incomingByID[req.ID] = req

			err = s.shouldShutDown(ErrServerClosing)
		}
	})

	if err != nil {
		c.processResult("acceptRequest", req, nil, err)
		return
	}

	if preempter != nil {
		result, err := preempter.Preempt(req.ctx, req.Request)
		if req.IsCall() && errors.Is(err, ErrAsyncResponse) {
			return
		}

		if !errors.Is(err, ErrNotHandled) {
			c.processResult("Preempt", req, result, err)
			return
		}
	}

	c.updateInFlight(func(s *inFlightState) {
		err = s.shouldShutDown(ErrServerClosing)
		if err != nil {
			return
		}
		s.handlerQueue = append(s.handlerQueue, req)
		if !s.handlerRunning {
			s.handlerRunning = true
			go c.handleAsync()
		}
	})

	if err != nil {
		c.processResult("acceptRequest", req, nil, err)
	}
}

func (c *Connection) processResult(from any, req *incomingRequest, result any, err error) error {
	var retErr error
	switch {
	case errors.Is(err, ErrAsyncResponse):
		if req.IsCall() {
			return nil
		}
		err = c.internalErrorf("%#v returned ErrAsyncResponse for a %q Request without an ID", from, req.Method)
		retErr = errors.Join(retErr, err)
	case errors.Is(err, ErrNotHandled), errors.Is(err, ErrMethodNotFound):
		err = fmt.Errorf("%w: %q", ErrMethodNotFound, req.Method)
	}

	if result != nil && err != nil {
		retErr = errors.Join(retErr, c.internalErrorf("%#v returned a non-nil result with a non-nil error for %s:\n%v\n%#v", from, req.Method, err, result))
		result = nil // Discard the spurious result and respond with err.
	}

	if req.IsCall() {
		res, respErr := NewResponse(req.ID, result, err)
		if respErr != nil {
			err = c.internalErrorf("%#v returned a malformed result for %q: %w", from, req.Method, respErr)
			retErr = errors.Join(retErr, err)
			res, respErr = NewResponse(req.ID, nil, err)
			if respErr != nil {
				retErr = errors.Join(retErr, c.internalErrorf("failed to marshal internal error response for %q: %w", req.Method, respErr))
			}
		}

		c.updateInFlight(func(s *inFlightState) {
			delete(s.incomingByID, req.ID)
		})

		if respErr == nil {
			writerErr := c.write(context.WithoutCancel(req.ctx), res)
			if writerErr != nil {
				retErr = errors.Join(retErr, writerErr)
			}
		}
	} else { // req is a notification
		if result != nil {
			err = c.internalErrorf("%#v returned a non-nil result for a %q Request without an ID", from, req.Method)
			retErr = errors.Join(retErr, err)
		} else if err != nil {
			err = fmt.Errorf("%w: %q notification failed: %v", ErrInternal, req.Method, err)
			retErr = errors.Join(retErr, err)
		}
		if err != nil {
			c.reportNotificationError(err)
		}
	}

	req.cancel(context.Canceled)
	c.updateInFlight(func(s *inFlightState) {
		if s.incoming == 0 {
			panic("jsonrpc2_v2: processResult called when incoming count is already zero")
		}
		s.incoming--
	})

	return retErr
}

func (c *Connection) handleAsync() {
	for {
		var req *incomingRequest
		c.updateInFlight(func(s *inFlightState) {
			if len(s.handlerQueue) > 0 {
				req, s.handlerQueue = s.handlerQueue[0], s.handlerQueue[1:]
			} else {
				s.handlerRunning = false
			}
		})
		if req == nil {
			return
		}

		// Only deliver to the Handler if not already canceled.
		if err := req.ctx.Err(); err != nil {
			if cause := context.Cause(req.ctx); cause != nil {
				err = cause
			}
			c.processResult("handleAsync", req, nil, err)
			continue
		}

		// The handler runs on its own goroutine, but the queue stays blocked
		// until it returns or calls Async, so handlers start in wire order and
		// can finish ordering-sensitive work before the next message is seen.
		r := &releaser{ch: make(chan struct{})}
		ctx := context.WithValue(req.ctx, asyncKey{}, r)
		go func() {
			defer r.release()
			result, err := c.handler.Handle(ctx, req.Request)
			c.processResult(c.handler, req, result, err)
		}()
		<-r.ch
	}
}

// Async lets the connection hand the next queued message to the Handler while
// the request whose context is ctx keeps running. Until Async is called (or
// Handle returns), messages are handled strictly one at a time in the order
// they arrived. Call it once any work that must happen before later messages
// are seen is done, e.g. right away for a long-running call.
//
// Async has no effect on contexts that are not from a Handle call, and later
// calls on the same request are no-ops.
func Async(ctx context.Context) {
	if r, ok := ctx.Value(asyncKey{}).(*releaser); ok {
		r.release()
	}
}

type asyncKey struct{}

// releaser unblocks the handler queue exactly once.
type releaser struct {
	once sync.Once
	ch   chan struct{}
}

func (r *releaser) release() { r.once.Do(func() { close(r.ch) }) }

func (c *Connection) readIncoming(ctx context.Context, reader Reader, preempter Preempter) {
	var err error

	for {
		var msg Message
		msg, err = reader.Read(ctx)
		if isMalformedMessage(err) {
			// The transport is intact but this frame is not a usable message.
			// JSON-RPC requires answering with a null id; keep reading.
			if writeErr := c.write(ctx, &Response{Error: err}); writeErr != nil {
				err = writeErr
				break
			}
			continue
		}
		if err != nil {
			break
		}

		switch msg := msg.(type) {
		case *Request:
			c.acceptRequest(ctx, msg, preempter)
		case *Response:
			var unexpected bool
			c.updateInFlight(func(s *inFlightState) {
				if ac, ok := s.outgoingCalls[msg.ID]; ok {
					delete(s.outgoingCalls, msg.ID)
					ac.retire(msg)
				} else {
					// dont call user internalErrorf here since user code could
					// call back into inFlightState which causes deadlock
					unexpected = true
				}
			})
			if unexpected {
				if msg.Error != nil {
					c.internalErrorf("received error response for unknown request ID %v: %v", msg.ID, msg.Error)
				} else {
					c.internalErrorf("received response for unknown request ID %v", msg.ID)
				}
			}

		default:
			c.internalErrorf("Read returned an unexpected message type: %T", msg)
		}
	}

	c.updateInFlight(func(s *inFlightState) {
		s.reading = false
		s.readErr = err

		// Retire any outgoing requests that were still in flight: with the Reader no
		// longer being processed, they necessarily cannot receive a response.
		for id, ac := range s.outgoingCalls {
			ac.retire(&Response{ID: id, Error: err})
		}
		s.outgoingCalls = nil

		// Cancel incoming requests still in flight: with the reader gone the
		// peer can no longer cancel them, and is most likely gone, so parked
		// handlers have nothing useful left to do.
		cause := fmt.Errorf("%w: %w", ErrServerClosing, err)
		for _, r := range s.incomingByID {
			r.cancel(cause)
		}
	})
}

// isMalformedMessage reports whether a Reader error rejects a single message
// (see DecodeMessage) rather than the transport.
func isMalformedMessage(err error) bool {
	return errors.Is(err, ErrParse) || errors.Is(err, ErrInvalidRequest)
}

func (c *Connection) start(ctx context.Context, reader Reader, preempter Preempter) {
	c.updateInFlight(func(s *inFlightState) {
		select {
		case <-c.done:
			// Bind already closed the connection; don't start a goroutine to read it
			return
		default:
		}

		// The goroutine started here will continue until the underlying stream is closed.
		//
		// (If the Binder closed the Connection already, this should error out and
		// return almost immediately.)
		s.reading = true
		go c.readIncoming(ctx, reader, preempter)
	})
}

func (c *Connection) write(ctx context.Context, msg Message) error {
	var writer Writer
	select {
	case writer = <-c.writer:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { c.writer <- writer }()
	err := writer.Write(ctx, msg)

	// if ctx.Err() is non-nil, the error is likely due to the context being
	// canceled, so we shouldn't treat it as a connection error, but if err !=
	// and ctx.Err() == nil, then call to write failed
	// and we should cancel incoming calls
	if err != nil && ctx.Err() == nil {
		c.updateInFlight(func(s *inFlightState) {
			if s.writeErr == nil {
				s.writeErr = err
				cause := fmt.Errorf("%w: %w", ErrServerClosing, err)
				for _, r := range s.incomingByID {
					r.cancel(cause)
				}
			}
		})
	}

	return err
}

// Notify invokes the target method, has no id and doesn't expect a response.
func (c *Connection) Notify(ctx context.Context, method string, params any) (err error) {
	attempted := false

	defer func() {
		if attempted {
			c.updateInFlight(func(s *inFlightState) {
				s.outgoingNotifications--
			})
		}
	}()

	c.updateInFlight(func(s *inFlightState) {
		// if there are still calls active a notification may be needed cancel
		// or manage them, so only shut down if no outgoing or incoming
		if len(s.outgoingCalls) == 0 && len(s.incomingByID) == 0 {
			err = s.shouldShutDown(ErrClientClosing)
			if err != nil {
				return
			}
		}
		s.outgoingNotifications++
		attempted = true
	})
	if err != nil {
		return err
	}

	notify, err := NewNotification(method, params)
	if err != nil {
		return fmt.Errorf("marshaling notify parameters: %w", err)
	}

	return c.write(ctx, notify)
}

func (c *Connection) internalErrorf(format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	if c.onInternalError == nil {
		panic("jsonrpc2: internal error: " + err.Error())
	}
	c.onInternalError(err)

	return fmt.Errorf("%w: %v", ErrInternal, err)
}

// Cancel cancels the Context passed to the Handle call for the inbound message
// with the given ID.
//
// Cancel will not complain if the ID is not a currently active message, and it
// will not cause any messages that have not arrived yet with that ID to be
// cancelled.
func (c *Connection) Cancel(id ID) {
	c.CancelCause(id, nil)
}

// CancelCause is like Cancel, but records cause as the reason the request's
// Context was canceled, readable through context.Cause. A nil cause reads as
// context.Canceled.
func (c *Connection) CancelCause(id ID, cause error) {
	var req *incomingRequest
	c.updateInFlight(func(s *inFlightState) {
		req = s.incomingByID[id]
	})
	if req != nil {
		req.cancel(cause)
	}
}

// Wait blocks until the connection is fully closed, but does not close it.
func (c *Connection) Wait() error {
	return c.wait(true)
}

func (c *Connection) wait(reportIOErrors bool) error {
	var err error
	<-c.done
	c.updateInFlight(func(s *inFlightState) {
		if reportIOErrors {
			if !errors.Is(s.readErr, io.EOF) {
				err = s.readErr
			}
			if err == nil && !errors.Is(s.writeErr, io.EOF) {
				err = s.writeErr
			}
		}
		if err == nil {
			err = s.closeErr
		}
	})
	return err
}

// Close shuts the connection down without waiting on the peer: it stops
// accepting new calls, cancels the Context of every incoming request still
// being handled, and fails outgoing calls still awaiting a response with
// ErrClientClosing. Once the handlers have returned (and their responses have
// been written) the transport is closed. Close blocks until then.
func (c *Connection) Close() error {
	c.updateInFlight(func(s *inFlightState) {
		s.connClosing = true
		for _, r := range s.incomingByID {
			r.cancel(ErrServerClosing)
		}
		for id, ac := range s.outgoingCalls {
			ac.retire(&Response{ID: id, Error: ErrClientClosing})
		}
		s.outgoingCalls = nil
	})
	return c.wait(false)
}

// inFlightState records the state of the incoming and outgoing calls on a
// Connection.
type inFlightState struct {
	connClosing bool  // true when the Connection's Close method has been called
	reading     bool  // true while the readIncoming goroutine is running
	readErr     error // non-nil when the readIncoming goroutine exits (typically io.EOF)
	writeErr    error // non-nil if a call to the Writer has failed with a non-canceled Context

	// closer shuts down and cleans up the Reader and Writer state, ideally
	// interrupting any Read or Write call that is currently blocked. It is closed
	// when the state is idle and one of: connClosing is true, readErr is non-nil,
	// or writeErr is non-nil.
	//
	// After the closer has been invoked, the closer field is set to nil
	// and the closeErr field is simultaneously set to its result.
	closer   io.Closer
	closeErr error // error returned from closer.Close

	outgoingCalls         map[ID]*PendingCall
	outgoingNotifications int // # of notifications awaiting "write"

	// incoming stores the total number of incoming calls and notifications
	// that have not yet written or processed a result.
	incoming     int
	incomingByID map[ID]*incomingRequest

	// handlerQueue stores the backlog of calls and notifications that were not
	// already handled by a preempter.
	// The queue does not include the request currently being handled (if any).
	handlerQueue   []*incomingRequest
	handlerRunning bool
}

func (s *inFlightState) idle() bool {
	return len(s.outgoingCalls) == 0 && s.outgoingNotifications == 0 && s.incoming == 0 && !s.handlerRunning
}

func (s *inFlightState) shouldShutDown(errClosing error) error {
	if s.connClosing {
		return errClosing
	}

	if s.readErr != nil {
		return fmt.Errorf("%w: %w", errClosing, s.readErr)
	}
	if s.writeErr != nil {
		return fmt.Errorf("%w: %w", errClosing, s.writeErr)
	}
	return nil
}

func (c *Connection) updateInFlight(f func(*inFlightState)) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	s := &c.state
	f(s)

	select {
	case <-c.done:
		// The connection was already completely done at the start of this call to
		// updateInFlight, so it must remain so. (The call to f should have noticed
		// that and avoided making any updates that would cause the state to be
		// non-idle.)
		if !s.idle() {
			panic("jsonrpc2_v2: updateInFlight transitioned to non-idle when already done")
		}

		return
	default:
	}

	if s.idle() && s.shouldShutDown(ErrUnknown) != nil {
		if s.closer != nil {
			s.closeErr = s.closer.Close()
			s.closer = nil
		}
		if s.reading {
			// The readIncoming goroutine is still running. Our call to Close should
			// cause it to exit soon, at which point it will make another call to
			// updateInFlight, set s.reading to false, and mark the Connection done.
		} else {
			// The readIncoming goroutine has exited, or never started to begin with.
			// Since everything else is idle, we're completely done.
			if c.onDone != nil {
				c.onDone()
			}
			close(c.done)
		}
	}
}

// async request tracks the state of an outgoing request until its response is
// received. request is from server to client
type PendingCall struct {
	id       ID
	ready    chan struct{} // closed after response has been set
	response *Response
}

func (ac *PendingCall) ID() ID {
	return ac.id
}

// check if result is already prepared
func (ac *PendingCall) IsReady() bool {
	select {
	case <-ac.ready:
		return true
	default:
		return false
	}
}

func (ac *PendingCall) retire(response *Response) {
	select {
	case <-ac.ready:
		panic(fmt.Sprintf("jsonrpc2: retire called twice for ID %v", ac.id))
	default:
	}

	ac.response = response
	// use close instead of sending value so that all waiters are unblocked
	// once closed, await is unblocked and response is available
	close(ac.ready)
}

func (ac *PendingCall) Await(ctx context.Context, result any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ac.ready:
	}

	if ac.response.Error != nil {
		return ac.response.Error
	}

	if result == nil {
		return nil
	}

	return json.Unmarshal(ac.response.Result, result)
}

type incomingRequest struct {
	*Request // the request being processed
	ctx      context.Context
	cancel   context.CancelCauseFunc
}

func (c *Connection) reportNotificationError(err error) {
	if c.onNotificationError != nil {
		c.onNotificationError(err)
	}
}
