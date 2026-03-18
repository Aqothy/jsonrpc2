package jsonrpc2

import (
	"encoding/json"
	"errors"
	"fmt"
)

// id can be either string, int or null according to specs, null id is discouraged
// null and missing ids will be treated the same and as notifications
type ID struct {
	value any
}

func MakeID(v any) (ID, error) {
	switch v := v.(type) {
	case nil:
		return ID{}, nil
	case float64:
		return Int64ID(int64(v)), nil
	case string:
		return StringID(v), nil
	}
	return ID{}, fmt.Errorf("%w: invalid ID type %T", ErrParse, v)
}

// MarshalJSON implements json.Marshaler.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.value)
}

// UnmarshalJSON implements json.Unmarshaler.
func (id *ID) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	newID, err := MakeID(v)
	if err != nil {
		return err
	}
	*id = newID
	return nil
}

// IsZero reports whether the ID is the zero value, for use with the omitzero JSON tag.
func (id ID) IsZero() bool {
	return !id.IsValid()
}

// interface for json rpc messages, both request and response
type Message interface {
	marshal(to *wireCombined)
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

func (msg *Request) marshal(to *wireCombined) {
	to.ID = msg.ID
	to.Method = msg.Method
	to.Params = msg.Params
}

func (msg *Response) NewResponse(id ID, result any, resErr error) (*Response, error) {
	responseRaw, err := marshalToRaw(msg.Result)
	return &Response{ID: id, Result: responseRaw, Error: resErr}, err
}

func (msg *Response) marshal(to *wireCombined) {
	to.ID = msg.ID
	to.Result = msg.Result
	to.Error = toWireError(msg.Error)
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
	result := &WireError{Message: err.Error()}
	var wrapped *WireError
	if errors.As(err, &wrapped) {
		// looks inside error and if it finds a wrapped WireError, it uses the code from it
		result.Code = wrapped.Code
	}
	return result
}

func EncodeMessage(msg Message) ([]byte, error) {
	wire := &wireCombined{RPCVersion: RPCVersion}
	msg.marshal(wire)
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshaling jsonrpc message: %w", err)
	}
	return data, nil
}

func DecodeMessage(data []byte) (Message, error) {
	msg := wireCombined{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshaling jsonrpc message: %w", err)
	}
	if msg.RPCVersion != RPCVersion {
		return nil, fmt.Errorf("invalid message version tag %q; expected %q", msg.RPCVersion, RPCVersion)
	}

	if msg.Method != "" {
		// has a method, must be a call
		return &Request{
			Method: msg.Method,
			ID:     msg.ID,
			Params: msg.Params,
		}, nil
	}

	// no method, should be a response

	if !msg.ID.IsValid() {
		return nil, ErrInvalidRequest
	}
	resp := &Response{
		ID:     msg.ID,
		Result: msg.Result,
	}
	// set error if there is one
	if msg.Error != nil {
		resp.Error = msg.Error
	}
	return resp, nil
}
