// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

// DefaultMaxMessageBytes is the incoming message size limit framers apply when
// none is configured.
const DefaultMaxMessageBytes = 32 << 20

// ErrMessageTooLarge is returned by a Reader when an incoming message exceeds
// its size limit. The stream cannot be resynchronized, so it is fatal.
var ErrMessageTooLarge = errors.New("JSON RPC message exceeds size limit")

func maxMessageBytes(n int) int {
	if n == 0 {
		return DefaultMaxMessageBytes
	}
	return n
}

// NdFramer frames newline-delimited JSON, as used by MCP and ACP over stdio:
// one complete JSON-RPC message per line. Blank lines are skipped.
type NdFramer struct {
	// MaxMessageBytes limits the size of one incoming message. Zero means
	// DefaultMaxMessageBytes; a negative value means no limit.
	MaxMessageBytes int
}

type ndReader struct {
	in  *bufio.Reader
	max int
}

type ndWriter struct {
	out io.Writer
}

// NewNdFramer returns an NdFramer with the default size limit.
func NewNdFramer() Framer { return NdFramer{} }

func (f NdFramer) Frame(rwc io.ReadWriteCloser) (Reader, Writer) {
	return &ndReader{in: bufio.NewReader(rwc), max: maxMessageBytes(f.MaxMessageBytes)}, &ndWriter{out: rwc}
}

func (r *ndReader) Read(ctx context.Context) (Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		line, err := r.readLine()
		if err != nil {
			if err == io.EOF && len(line) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		return DecodeMessage(line)
	}
}

// readLine reads through the next newline without buffering more than the
// size limit (plus room for a CR LF terminator).
func (r *ndReader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.in.ReadSlice('\n')
		if r.max > 0 && len(line)+len(chunk) > r.max+2 {
			return nil, ErrMessageTooLarge
		}
		line = append(line, chunk...)
		if err != bufio.ErrBufferFull {
			return line, err
		}
	}
}

func (w *ndWriter) Write(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	data, err := EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	data = append(data, '\n')

	_, err = w.out.Write(data)
	return err
}

// HeaderFramer frames messages behind a Content-Length header, as used by LSP.
type HeaderFramer struct {
	// MaxMessageBytes limits the size of one incoming message. Zero means
	// DefaultMaxMessageBytes; a negative value means no limit.
	MaxMessageBytes int
}

type headerReader struct {
	in  *bufio.Reader
	max int
}

type headerWriter struct {
	out io.Writer
}

// NewHeaderFramer returns a HeaderFramer with the default size limit.
func NewHeaderFramer() Framer { return HeaderFramer{} }

func (f HeaderFramer) Frame(rwc io.ReadWriteCloser) (Reader, Writer) {
	return &headerReader{in: bufio.NewReader(rwc), max: maxMessageBytes(f.MaxMessageBytes)}, &headerWriter{rwc}
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
	if r.max > 0 && contentLength > int64(r.max) {
		return nil, ErrMessageTooLarge
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
