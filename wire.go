// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2

import "encoding/json"

// types used in jsonrpc 2 from the spec: https://www.jsonrpc.org/specification

var (
	// invalid json received by the server
	ErrParse = NewError(-32700, "JSON RPC parse error")
	// json sent is not valid request
	ErrInvalidRequest = NewError(-32600, "JSON RPC invalid request")
	// method does not exist / is not available
	ErrMethodNotFound = NewError(-32601, "JSON RPC method not found")
	// invalid method params
	ErrInvalidParams = NewError(-32602, "JSON RPC invalid params")
	// internal json rpc error
	ErrInternal = NewError(-32603, "JSON RPC internal error")

	// - 32000 to 32099 are reserved for implementation defined server errors
	// ErrServerOverloaded is returned when a message was refused due to a
	// server being temporarily unable to accept any new messages.
	ErrServerOverloaded = NewError(-32000, "JSON RPC overloaded")
	// ErrUnknown should be used for all non coded errors.
	ErrUnknown = NewError(-32001, "JSON RPC unknown error")
	// ErrServerClosing is returned for calls that arrive while the server is closing.
	ErrServerClosing = NewError(-32002, "JSON RPC server is closing")
	// ErrClientClosing is a dummy error returned for calls initiated while the client is closing.
	ErrClientClosing = NewError(-32003, "JSON RPC client is closing")
)

// NOTE: json.RawMessage is used instead of []byte so that we can unmarshal the json later when we know the type

const RPCVersion = "2.0"

// wireRequest is an outgoing call or notification; notifications omit the id.
type wireRequest struct {
	RPCVersion string          `json:"jsonrpc"`
	ID         ID              `json:"id,omitzero"`
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params,omitempty"`
}

// wireResponse is an outgoing response. The id is always present: JSON-RPC
// requires null when the request's id could not be determined.
type wireResponse struct {
	RPCVersion string          `json:"jsonrpc"`
	ID         ID              `json:"id"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      *WireError      `json:"error,omitempty"`
}

// wireIncoming decodes any incoming message. Members are raw so a missing
// member can be told apart from an explicit null.
type wireIncoming struct {
	RPCVersion *string         `json:"jsonrpc"`
	ID         json.RawMessage `json:"id"`
	Method     *string         `json:"method"`
	Params     json.RawMessage `json:"params"`
	Result     json.RawMessage `json:"result"`
	Error      json.RawMessage `json:"error"`
}

// Error from response
type WireError struct {
	Code    int64           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Used to build error for application
func NewError(code int64, message string) error {
	return &WireError{
		Code:    code,
		Message: message,
	}
}

func (err *WireError) Error() string {
	return err.Message
}

// checks if error and code are the same
func (err *WireError) Is(other error) bool {
	w, ok := other.(*WireError)
	if !ok || w == nil {
		return false
	}
	return err.Code == w.Code
}
