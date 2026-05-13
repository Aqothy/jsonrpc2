package jsonrpc2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
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
}

// connection config for bidirectional jsonrpc2 connection
type ConnectionConfig struct {
	Reader          Reader
	Writer          Writer
	Closer          io.Closer
	Preempter       Preempter
	Bind            func(*Connection) Handler
	OnDone          func()
	OnInternalError func(error)
}

// Bind returns the options unmodified.
func (o ConnectionOptions) Bind(context.Context, *Connection) ConnectionOptions {
	return o
}

// low level, provide everything yourself like reader, writer, etc, no framer provided
func NewConnection(ctx context.Context, cfg ConnectionConfig) *Connection {
	ctx = notDone{ctx}

	c := &Connection{
		state:           inFlightState{closer: cfg.Closer},
		done:            make(chan struct{}),
		writer:          make(chan Writer, 1),
		onDone:          cfg.OnDone,
		onInternalError: cfg.OnInternalError,
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

	onInternalError func(error)
	onDone          func()
}

func (c *Connection) start(ctx context.Context, reader Reader, preempter Preempter) {
}

func (c *Connection) write(ctx context.Context, msg Message) error {
	writer := <-c.writer
	defer func() { c.writer <- writer }()
	err := writer.Write(ctx, msg)

	return err
}

// high level, provides default header framer
func bindConnection(bindCtx context.Context, rwc io.ReadWriteCloser, binder Binder, onDone func()) *Connection {
	ctx := notDone{bindCtx}

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
	cancel   context.CancelFunc
}

// notDone wraps context and returns nil for Done so that the connection's read
// loop will not exit until the connection is closed or an error occurs, even if
// the context passed to NewConnection is canceled. This allows the caller to
// use a context with a timeout or deadline without worrying about it causing
// the connection to close prematurely and losing messages that are in flight.
// The connection will still respect the context's values, so it can be used to
// pass request-scoped values to handlers.
type notDone struct {
	ctx context.Context
}

func (ic notDone) Value(key any) any {
	return ic.ctx.Value(key)
}

func (ic notDone) Done() <-chan struct{} {
	return nil
}
func (ic notDone) Err() error {
	return nil
}
func (ic notDone) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}
