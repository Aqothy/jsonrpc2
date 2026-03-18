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
)

// NOTE: json.RawMessage is used instead of []byte so that we can unmarshal the json later when we know the type

const RPCVersion = "2.0"

// combination of request and response
// we combine them since jsonrpc 2 can send both so we don't know which
// we used a combined struct so we can decode and check which it is
type wireCombined struct {
	RPCVersion string           `json:"jsonrpc"`
	ID         ID               `json:"id,omitzero"`
	Method     string           `json:"method,omitempty"`
	Params     json.RawMessage  `json:"params,omitempty"`
	Result     *json.RawMessage `json:"result,omitempty"`
	Error      *WireError       `json:"error,omitempty"`
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
	if !ok {
		return false
	}
	return err.Code == w.Code
}
