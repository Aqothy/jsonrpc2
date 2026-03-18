package jsonrpc2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func NewFramer() Framer {
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
