package jsonrpc2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/textproto"
	"strconv"
)

type Reader interface {
	Read(context.Context) (Message, error)
}

type Writer interface {
	Write(context.Context, Message) error
}

type Framer interface {
	Frame(io.ReadWriteCloser) (Reader, Writer)
}

// reads raw json messages without framing (Content-Length header)
type rawFramer struct{}

type rawReader struct {
	in *json.Decoder
}

type rawWriter struct {
	out io.Writer
}

func NewRawFramer() Framer {
	return rawFramer{}
}

func (f rawFramer) Frame(rwc io.ReadWriteCloser) (Reader, Writer) {
	return &rawReader{json.NewDecoder(rwc)}, &rawWriter{rwc}
}

func (r *rawReader) Read(ctx context.Context) (Message, error) {
	// if context is done, return error immediately, else just move on
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var raw json.RawMessage
	// continues reading till it gets a full json message, then decode it
	if err := r.in.Decode(&raw); err != nil {
		return nil, err
	}
	msg, err := DecodeMessage(raw)
	return msg, err
}

func (w *rawWriter) Write(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	data, err := EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	_, err = w.out.Write(data)
	return err
}

// messages with framed headers, where each message is preceded by a Content-Length header specifying the length of the message in bytes
// used by lsp and acp
type headerFramer struct{}

type headerReader struct {
	in *bufio.Reader
}

type headerWriter struct {
	out io.Writer
}

func NewHeaderFramer() Framer { return headerFramer{} }

func (f headerFramer) Frame(rwc io.ReadWriteCloser) (Reader, Writer) {
	return &headerReader{bufio.NewReader(rwc)}, &headerWriter{rwc}
}

func (r *headerReader) Read(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	tp := textproto.NewReader(r.in)
	headers, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}

	clStr := headers.Get("Content-Length")
	contentLength, err := strconv.ParseInt(clStr, 10, 32)
	if err != nil || contentLength <= 0 {
		return nil, fmt.Errorf("invalid Content-Length: %v", clStr)
	}

	data := make([]byte, contentLength)
	if _, err := io.ReadFull(r.in, data); err != nil {
		return nil, err
	}

	return DecodeMessage(data)
}

// writes the message with a Content-Length header, then the json data
// looks like:
// Content-Length: ...\r\n
// \r\n
//
//	{
//		"jsonrpc": "2.0",
//		"id": 1,
//		"method": "foo",
//		"params": {
//			...
//		}
//	}
func (w *headerWriter) Write(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	data, err := EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	_, err = fmt.Fprintf(w.out, "Content-Length: %v\r\n\r\n", len(data))
	if err == nil {
		_, err = w.out.Write(data)
	}
	return err
}
