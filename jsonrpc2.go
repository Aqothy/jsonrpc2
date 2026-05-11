package jsonrpc2

import (
	"errors"
)

var (
	ErrIdleTimeout = errors.New("Timed out waiting for new connections")

	// if ErrNotHandled then server replies with ErrMethodNotFound
	ErrNotHandled = errors.New("JSON RPC request not handled")

	// not an actual error, just indicates that it will generate response
	// asynchronously
	ErrAsyncResponse = errors.New("JSON RPC asynchronous response")
)
