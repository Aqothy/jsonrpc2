# jsonrpc2

A small Go JSON-RPC 2.0 connection package with stream framers for LSP, MCP, ACP-style newline transports, and message-oriented WebSocket transports.

```sh
go get github.com/Aqothy/jsonrpc2
```

## Transport mapping

- LSP stdio: `Content-Length` headers, use `NewHeaderFramer`.
- MCP stdio: one UTF-8 JSON-RPC message per newline with no embedded newlines, use `NewNdFramer`.
- ACP stdio: one UTF-8 JSON-RPC message per newline with no embedded newlines, use `NewNdFramer`.
- WebSocket: one JSON-RPC message per text WebSocket message, use `NewWebSocketConnection` with an adapter.

## Differences from Go tools's internal jsonrpc2_v2

This package is based on Go's `golang.org/x/tools/internal/jsonrpc2_v2`, with
several changes adapted for my usage, only including what I need and
extracted to be used as a standalone library

- Added newline-delimited framing with `NewNdFramer` for MCP/ACP-style stdio transports.
- Added WebSocket transport helpers via a small adapter interface.
- Resolved all TODO from the original implementation
- Using newer golang apis
- Handlers run on their own goroutine and can call `jsonrpc2.Async(ctx)` to let
  later messages be handled concurrently (as in the MCP Go SDK's fork).
- Malformed input (invalid JSON, non-object values such as batches, blank lines)
  is answered with a `null`-id error response, or skipped, without closing the
  connection. A malformed response fails its pending call instead of hanging it,
  and responses are never answered, so two peers can't bounce errors forever.
- Framers cap incoming messages at `DefaultMaxMessageBytes` (32 MiB); set
  `NdFramer{MaxMessageBytes: n}` / `HeaderFramer{MaxMessageBytes: n}` to change it.
- `Close` doesn't wait on the peer: it cancels in-flight handlers and fails
  pending calls, then closes the transport once the handlers return. Losing the
  read side (peer gone) also cancels in-flight handlers. Causes are readable via
  `context.Cause`, and `CancelCause` records one explicitly.

## Concurrency

`Handle` is called for each message in arrival order. The next message is not
dispatched until the current handler returns or calls `jsonrpc2.Async(ctx)`, so
ordering-sensitive work (for example registering state that a later
notification will look up) can run before releasing the queue:

```go
Handler: jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
	if req.IsCall() {
		jsonrpc2.Async(ctx) // long-running calls must not block later messages
	}
	return handle(ctx, req)
})
```

Notifications that never call `Async` keep their relative order.

## Future plans

- [] maybe support batching (LSP never uses it, MCP removed it in 2025-06-18,
  and stable ACP rejects it)

## LSP over stdio

LSP uses `Content-Length` headers. `NewHeaderFramer` is the default for `NewStreamConnection`, but it is shown explicitly here.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Aqothy/jsonrpc2"
)

type stdioRWC struct {
	io.Reader
	io.Writer
}

func (stdioRWC) Close() error { return nil }

func main() {
	ctx := context.Background()
	rwc := stdioRWC{Reader: os.Stdin, Writer: os.Stdout}

	conn := jsonrpc2.NewStreamConnection(ctx, rwc, jsonrpc2.ConnectionOptions{
		Framer: jsonrpc2.NewHeaderFramer(),
		Handler: jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			switch req.Method {
			case "initialize":
				return map[string]any{
					"capabilities": map[string]any{
						"textDocumentSync": 1,
					},
					"serverInfo": map[string]any{"name": "example-lsp", "version": "0.1.0"},
				}, nil
			case "initialized", "textDocument/didOpen", "textDocument/didChange":
				return nil, nil // notifications do not get responses
			default:
				return nil, fmt.Errorf("%w: %s", jsonrpc2.ErrMethodNotFound, req.Method)
			}
		}),
		OnInternalError:     func(err error) { log.Printf("jsonrpc internal error: %v", err) },
		OnNotificationError: func(err error) { log.Printf("notification error: %v", err) },
	})

	if err := conn.Wait(); err != nil {
		log.Fatal(err)
	}
}
```

## MCP over stdio

MCP stdio messages are newline-delimited JSON-RPC messages and must not contain embedded newlines, so use the strict newline-delimited framer.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Aqothy/jsonrpc2"
)

type stdioRWC struct{ io.Reader; io.Writer }
func (stdioRWC) Close() error { return nil }

func main() {
	ctx := context.Background()
	rwc := stdioRWC{Reader: os.Stdin, Writer: os.Stdout}

	conn := jsonrpc2.NewStreamConnection(ctx, rwc, jsonrpc2.ConnectionOptions{
		Framer: jsonrpc2.NewNdFramer(),
		Handler: jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			switch req.Method {
			case "initialize":
				return map[string]any{
					"protocolVersion": "2025-06-18",
					"serverInfo":      map[string]any{"name": "example-mcp", "version": "0.1.0"},
					"capabilities":    map[string]any{"tools": map[string]any{}},
				}, nil
			case "notifications/initialized":
				return nil, nil
			case "tools/list":
				return map[string]any{
					"tools": []map[string]any{{
						"name":        "echo",
						"description": "Echo text back to the caller",
						"inputSchema": map[string]any{
							"type":       "object",
							"properties": map[string]any{"text": map[string]any{"type": "string"}},
						},
					}},
				}, nil
			case "tools/call":
				var params struct {
					Name      string         `json:"name"`
					Arguments map[string]any `json:"arguments"`
				}
				if err := json.Unmarshal(req.Params, &params); err != nil {
					return nil, fmt.Errorf("%w: %v", jsonrpc2.ErrInvalidParams, err)
				}
				return map[string]any{
					"content": []map[string]any{{"type": "text", "text": fmt.Sprint(params.Arguments["text"])}},
					"isError": false,
				}, nil
			default:
				return nil, jsonrpc2.ErrNotHandled
			}
		}),
	})

	if err := conn.Wait(); err != nil {
		log.Fatal(err)
	}
}
```

## ACP over newline-delimited JSON

ACP-style transports can use the strict newline-delimited framer.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Aqothy/jsonrpc2"
)

type stdioRWC struct{ io.Reader; io.Writer }
func (stdioRWC) Close() error { return nil }

func main() {
	ctx := context.Background()
	rwc := stdioRWC{Reader: os.Stdin, Writer: os.Stdout}

	conn := jsonrpc2.NewStreamConnection(ctx, rwc, jsonrpc2.ConnectionOptions{
		Framer: jsonrpc2.NewNdFramer(),
		Handler: jsonrpc2.HandlerFunc(func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			switch req.Method {
			case "initialize":
				return map[string]any{
					"protocolVersion": 1,
					"agentCapabilities": map[string]any{
						"loadSession": false,
					},
				}, nil
			case "session/new":
				return map[string]any{"sessionId": "example-session"}, nil
			case "session/prompt":
				var params struct {
					SessionID string           `json:"sessionId"`
					Prompt    []map[string]any `json:"prompt"`
				}
				if err := json.Unmarshal(req.Params, &params); err != nil {
					return nil, fmt.Errorf("%w: %v", jsonrpc2.ErrInvalidParams, err)
				}
				return map[string]any{"stopReason": "end_turn"}, nil
			case "session/cancel":
				return nil, nil
			default:
				return nil, jsonrpc2.ErrNotHandled
			}
		}),
	})

	if err := conn.Wait(); err != nil {
		log.Fatal(err)
	}
}
```

## WebSocket transport

`NewWebSocketConnection` accepts any type implementing this package's `WebSocket` interface. This adapter uses `github.com/coder/websocket` text messages.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Aqothy/jsonrpc2"
	"github.com/coder/websocket"
)

type coderJSONRPCSocket struct{ c *websocket.Conn }

func (s coderJSONRPCSocket) ReadMessage(ctx context.Context) ([]byte, error) {
	typ, data, err := s.c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("expected text websocket message, got %v", typ)
	}
	return data, nil
}

func (s coderJSONRPCSocket) WriteMessage(ctx context.Context, data []byte) error {
	return s.c.Write(ctx, websocket.MessageText, data)
}

func (s coderJSONRPCSocket) Close() error {
	return s.c.Close(websocket.StatusNormalClosure, "")
}

func main() {
	ctx := context.Background()
	ws, _, err := websocket.Dial(ctx, "wss://example.com/jsonrpc", nil)
	if err != nil {
		log.Fatal(err)
	}

	conn := jsonrpc2.NewWebSocketConnection(ctx, coderJSONRPCSocket{c: ws}, jsonrpc2.HandlerFunc(
		func(ctx context.Context, req *jsonrpc2.Request) (any, error) {
			if req.Method == "ping" {
				return "pong", nil
			}
			return nil, jsonrpc2.ErrNotHandled
		},
	))
	defer conn.Close()

	var result string
	if err := conn.Call(ctx, "ping", "hello").Await(ctx, &result); err != nil {
		log.Fatal(err)
	}
	log.Printf("result: %s", result)
}
```
