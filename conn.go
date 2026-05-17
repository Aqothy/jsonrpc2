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

// Binder builds a connection configuration.
// This may be used in servers to generate a new configuration per connection.
// ConnectionOptions itself implements Binder returning itself unmodified, to
// allow for the simple cases where no per connection information is needed.
type Binder interface {
	// Bind returns the ConnectionOptions to use when establishing the passed-in
	// Connection.
	//
	// The connection is not ready to use when Bind is called,
	// but Bind may close it without reading or writing to it.
	Bind(context.Context, *Connection) ConnectionOptions
}

type BinderFunc func(context.Context, *Connection) ConnectionOptions

func (f BinderFunc) Bind(ctx context.Context, c *Connection) ConnectionOptions {
	return f(ctx, c)
}

var _ Binder = BinderFunc(nil)

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

// Bind returns the options unmodified.
func (o ConnectionOptions) Bind(context.Context, *Connection) ConnectionOptions {
	return o
}

// low level, provide everything yourself like reader, writer, etc, no framer provided
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

	c.handler = cfg.Bind(c)
	c.writer <- cfg.Writer
	c.start(ctx, cfg.Reader, cfg.Preempter)

	return c
}

// Connection manages the jsonrpc2 protocol, connecting responses back to their
// calls.
// Connection is bidirectional; it does not have a designated server or client
// end.
type Connection struct {
	seq int64 // must only be accessed using atomic operations

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
// You dont have to wait for resopnse, can be ignored if not needed
// If sending call failed then resopnse will be ready and have the error in it
func (c *Connection) Call(ctx context.Context, method string, params any) *PendingCall {
	id := Int64ID((atomic.AddInt64(&c.seq, 1)))

	ac := &PendingCall{
		id:    id,
		ready: make(chan struct{}),
	}

	// When this method returns, either ac is retired, or the request has been
	// written successfully and the call is awaiting a response (to be provided by
	// the readIncoming goroutine).

	call, err := NewCall(ac.id, method, params)
	if err != nil {
		ac.retire(&Response{ID: id, Error: fmt.Errorf("marshalling call parameters: %w", err)})
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
		c.updateInFlight(func(s *inFlightState) {
			if s.outgoingCalls[ac.id] == ac {
				delete(s.outgoingCalls, ac.id)
				ac.retire(&Response{ID: id, Error: err})
			} else {
				// ac was already retired by the readIncoming goroutine:
				// perhaps our write raced with the Read side of the connection breaking.
			}
		})
	}

	return ac
}

// Reponds to incoming call.
func (c *Connection) Respond(id ID, result any, err error) error {
	var req *incomingRequest
	c.updateInFlight(func(s *inFlightState) {
		req = s.incomingByID[id]
	})
	if req == nil {
		return c.internalErrorf("Request not found for ID %v", id)
	}

	if err == ErrAsyncResponse {
		err = c.internalErrorf("Respond called with ErrAsyncReponse for %q", req.Method)
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
		}

		err = s.shouldShutDown(ErrServerClosing)
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
	switch err {
	case ErrAsyncResponse:
		if !req.IsCall() {
			return c.internalErrorf("%#v returned ErrAsyncResponse for a %q Request without an ID", from, req.Method)
		}
		return nil
	case ErrNotHandled, ErrMethodNotFound:
		err = fmt.Errorf("%w: %q", ErrMethodNotFound, req.Method)
	}

	if result != nil && err != nil {
		c.internalErrorf("%#v returned a non-nil result with a non-nil error for %s:\n%v\n%#v", from, req.Method, err, result)
		result = nil // Discard the spurious result and respond with err.
	}

	if req.IsCall() {
		if result == nil && err == nil {
			err = c.internalErrorf("%#v returned a nil result and nil error for a %q Request that requires a Response", from, req.Method)
		}

		res, respErr := NewResponse(req.ID, result, err)

		c.updateInFlight(func(s *inFlightState) {
			delete(s.incomingByID, req.ID)
		})

		if respErr == nil {
			writerErr := c.write(context.WithoutCancel(req.ctx), res)
			if err == nil {
				err = writerErr
			}
		} else {
			err = c.internalErrorf("%#v returned a malformed result for %q: %w", from, req.Method, respErr)
		}
	} else { // req is a notification
		if result != nil {
			err = c.internalErrorf("%#v returned a non-nil result for a %q Request without an ID", from, req.Method)
		} else if err != nil {
			err = fmt.Errorf("%w: %q notification failed: %v", ErrInternal, req.Method, err)
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

	return nil
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

		result, err := c.handler.Handle(req.ctx, req.Request)
		c.processResult(c.handler, req, result, err)
	}

}

func (c *Connection) readIncoming(ctx context.Context, reader Reader, preempter Preempter) {
	var err error

	for {
		var msg Message
		msg, err = reader.Read(ctx)
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
				c.internalErrorf("received response for unknown request ID %v", msg.ID)
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
	})
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
	writer := <-c.writer
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
				cause := fmt.Errorf("%w: %v", ErrServerClosing, err)
				for _, r := range s.incomingByID {
					r.cancel(cause)
				}
			}
		})
	}

	return err
}

// Notify invokes the target method, has no id and doesn't expact response.
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
		return fmt.Errorf("marshaling notify parameters: %v", err)
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
	var req *incomingRequest
	c.updateInFlight(func(s *inFlightState) {
		req = s.incomingByID[id]
	})
	if req != nil {
		req.cancel(context.Canceled)
	}
}

// Wait blocks until the connection is fully closed, but does not close it.
func (c *Connection) Wait() error {
	var err error
	<-c.done
	c.updateInFlight(func(s *inFlightState) {
		err = s.closeErr
	})
	return err
}

func (c *Connection) Close() error {
	// Stop handling new requests, and interrupt the reader (by closing the
	// connection) as soon as the active requests finish.
	c.updateInFlight(func(s *inFlightState) { s.connClosing = true })
	return c.Wait()
}

// high level, provides default header framer
func bindConnection(bindCtx context.Context, rwc io.ReadWriteCloser, binder Binder, onDone func()) *Connection {
	ctx := context.WithoutCancel(bindCtx)

	c := &Connection{
		state:  inFlightState{closer: rwc},
		done:   make(chan struct{}),
		writer: make(chan Writer, 1),
		onDone: onDone,
	}

	options := binder.Bind(ctx, c)
	framer := options.Framer
	if framer == nil {
		framer = NewHeaderFramer()
	}

	c.handler = options.Handler
	if c.handler == nil {
		c.handler = defaultHandler{}
	}

	c.onInternalError = options.OnInternalError
	c.onNotificationError = options.OnNotificationError

	reader, writer := framer.Frame(rwc)
	c.writer <- writer
	c.start(ctx, reader, options.Preempter)
	return c
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
		return fmt.Errorf("%w: %v", errClosing, s.readErr)
	}
	if s.writeErr != nil {
		return fmt.Errorf("%w: %v", errClosing, s.writeErr)
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

		} else {
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
