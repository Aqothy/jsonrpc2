// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// id can be either string or int. JSON-RPC permits null IDs, but they are discouraged and not supported.
// A zero ID means the id member is absent; JSON null IDs are decoded as zero IDs.
type ID struct {
	value any
}

// MakeID coerces a JSON-RPC id value into an ID. A nil value means no ID.
func MakeID(v any) (ID, error) {
	switch v := v.(type) {
	case nil:
		return ID{}, nil
	case ID:
		return v, nil
	case json.Number:
		return parseNumberID(v.String())
	case string:
		return StringID(v), nil
	case float64:
		const maxSafeInteger = 1 << 53
		if math.Trunc(v) != v || v < -maxSafeInteger || v > maxSafeInteger {
			return ID{}, fmt.Errorf("%w: invalid numeric ID %v", ErrParse, v)
		}
		return Int64ID(int64(v)), nil
	case int:
		return Int64ID(int64(v)), nil
	case int64:
		return Int64ID(v), nil
	}
	return ID{}, fmt.Errorf("%w: invalid ID type %T", ErrParse, v)
}

func parseNumberID(s string) (ID, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ID{}, fmt.Errorf("%w: invalid numeric ID %q", ErrParse, s)
	}
	return Int64ID(i), nil
}

// MarshalJSON implements json.Marshaler.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.value)
}

// UnmarshalJSON implements json.Unmarshaler.
func (id *ID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*id = ID{}
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*id = StringID(s)
		return nil
	}

	newID, err := parseNumberID(string(data))
	if err == nil {
		*id = newID
	}
	return err
}

// IsZero reports whether the ID is the zero value, for use with the omitzero JSON tag.
func (id ID) IsZero() bool {
	return !id.IsValid()
}

// interface for json rpc messages, both request and response
type Message interface {
	wire() any
}

// Message from client to server, either a call or a notification
// If it has an ID it is a call, otherwise it is a notification
type Request struct {
	// ID of this request, used to tie the Response back to the request
	ID ID
	// Method is a string containing the method name to invoke
	Method string
	// Params is either a struct or an array with the parameters of the method
	Params json.RawMessage
}

// Message from server to client, either a response to a call or an error
// It will have the same ID as the call it is a response to
type Response struct {
	// result is the content of the response
	Result json.RawMessage
	// err is set only if the call failed
	Error error
	// id of the request this is a response to
	ID ID
}

func StringID(s string) ID { return ID{value: s} }

func Int64ID(i int64) ID { return ID{value: i} }

// The default value for ID will return false.
func (id ID) IsValid() bool { return id.value != nil }

func (id ID) String() string {
	switch v := id.value.(type) {
	case nil:
		return "<missing>"
	case string:
		return strconv.Quote(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

// Raw returns the underlying Go value for the ID. For the zero ID, Raw returns nil.
func (id ID) Raw() any { return id.value }

func marshalToRaw(obj any) (json.RawMessage, error) {
	if obj == nil {
		return nil, nil
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func NewNotification(method string, params any) (*Request, error) {
	paramsRaw, err := marshalToRaw(params)
	return &Request{Method: method, Params: paramsRaw}, err
}

func NewCall(id ID, method string, params any) (*Request, error) {
	paramsRaw, err := marshalToRaw(params)
	return &Request{ID: id, Method: method, Params: paramsRaw}, err
}

// valid ids are calls (need to be responded to) and invalid ids are notifications (don't need to be responded to)
func (msg *Request) IsCall() bool { return msg.ID.IsValid() }

func (msg *Request) wire() any {
	return &wireRequest{RPCVersion: RPCVersion, ID: msg.ID, Method: msg.Method, Params: msg.Params}
}

// NewResponse constructs a response. If resErr is nil and result is nil,
// the response is a successful JSON-RPC response with result:null.
func NewResponse(id ID, result any, resErr error) (*Response, error) {
	if resErr != nil {
		return &Response{ID: id, Error: resErr}, nil
	}
	if result == nil {
		return &Response{ID: id, Result: json.RawMessage("null")}, nil
	}
	responseRaw, err := marshalToRaw(result)
	return &Response{ID: id, Result: responseRaw}, err
}

func (msg *Response) wire() any {
	w := &wireResponse{RPCVersion: RPCVersion, ID: msg.ID}
	switch {
	case msg.Error != nil:
		w.Error = toWireError(msg.Error)
	case msg.Result == nil:
		w.Result = json.RawMessage("null")
	default:
		w.Result = msg.Result
	}
	return w
}

func toWireError(err error) *WireError {
	if err == nil {
		// no error, the response is complete
		return nil
	}
	if err, ok := err.(*WireError); ok {
		// already a wire error, just use it
		return err
	}
	result := &WireError{Code: wireErrorCode(ErrUnknown), Message: err.Error()}
	var wrapped *WireError
	if errors.As(err, &wrapped) {
		// looks inside error and if it finds a wrapped WireError, it uses the code from it
		result.Code = wrapped.Code
	}
	return result
}

func wireErrorCode(err error) int64 {
	var wire *WireError
	if errors.As(err, &wire) {
		return wire.Code
	}
	return 0
}

func EncodeMessage(msg Message) ([]byte, error) {
	data, err := json.Marshal(msg.wire())
	if err != nil {
		return nil, fmt.Errorf("marshaling jsonrpc message: %w", err)
	}
	return data, nil
}

// DecodeMessage decodes one JSON-RPC message.
//
// Errors wrapping ErrParse or ErrInvalidRequest mean the data was not a usable
// message; a Connection answers those with an error response whose id is
// null and keeps reading. Response-shaped input is never answered: a
// malformed response is returned as a *Response carrying an error, so a
// matching pending call fails instead of hanging, and a response whose id is
// missing or null is reported as unexpected.
//
// Decoding is lenient where it is safe to be: a missing "jsonrpc" member is
// accepted, and "error": null next to a result is ignored.
func DecodeMessage(data []byte) (Message, error) {
	var msg wireIncoming
	if err := json.Unmarshal(data, &msg); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			// Valid JSON, but not a message object (e.g. a batch array).
			return nil, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrParse, err)
	}
	if msg.RPCVersion != nil && *msg.RPCVersion != RPCVersion {
		return nil, fmt.Errorf("%w: version tag %q, expected %q", ErrInvalidRequest, *msg.RPCVersion, RPCVersion)
	}
	var id ID
	var idErr error
	if len(msg.ID) > 0 {
		idErr = id.UnmarshalJSON(msg.ID)
	}

	if msg.Method != nil {
		if idErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrParse, idErr)
		}
		return &Request{ID: id, Method: *msg.Method, Params: msg.Params}, nil
	}

	if msg.ID == nil && msg.Result == nil && msg.Error == nil {
		return nil, fmt.Errorf("%w: message has neither a method nor a response member", ErrInvalidRequest)
	}

	resp := &Response{ID: id}
	switch {
	case idErr != nil:
		resp.ID = ID{}
		resp.Error = fmt.Errorf("%w: response id: %v", ErrInvalidRequest, idErr)
	case len(msg.Error) > 0 && string(msg.Error) != "null":
		var wireErr WireError
		if err := json.Unmarshal(msg.Error, &wireErr); err != nil {
			resp.Error = fmt.Errorf("%w: malformed response error: %v", ErrInvalidRequest, err)
		} else {
			resp.Error = &wireErr
		}
	case msg.Result != nil:
		resp.Result = msg.Result
	default:
		resp.Error = fmt.Errorf("%w: response has neither result nor error", ErrInvalidRequest)
	}
	return resp, nil
}
