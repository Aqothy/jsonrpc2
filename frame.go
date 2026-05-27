// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonrpc2

import (
	"bufio"
	"bytes"
	"context"
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

// strict newline-delimited framer for protocols like MCP stdio and ACP stdio.
// It requires exactly one complete JSON-RPC message per line.
type ndFramer struct{}

type ndReader struct {
	in *bufio.Reader
}

type ndWriter struct {
	out io.Writer
}

func NewNdFramer() Framer { return ndFramer{} }

func (f ndFramer) Frame(rwc io.ReadWriteCloser) (Reader, Writer) {
	return &ndReader{bufio.NewReader(rwc)}, &ndWriter{out: rwc}
}

func (r *ndReader) Read(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	line, err := r.in.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	line = bytes.TrimRight(line, "\r\n")
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, fmt.Errorf("empty newline-delimited JSON-RPC message")
	}
	return DecodeMessage(line)
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

// messages with framed headers, where each message is preceded by a Content-Length header specifying the length of the message in bytes
// used by LSP
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
