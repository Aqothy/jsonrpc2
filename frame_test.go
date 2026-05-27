package jsonrpc2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
)

type mockRWC struct {
	io.Reader
	io.Writer
}

func (m *mockRWC) Close() error { return nil }

func TestNdFramer(t *testing.T) {
	ctx := context.Background()
	framer := NewNdFramer()
	buf := &bytes.Buffer{}
	rwc := &mockRWC{Reader: buf, Writer: buf}
	r, w := framer.Frame(rwc)

	msg, _ := NewNotification("test", nil)
	if err := w.Write(ctx, msg); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if got := buf.String(); got == "" || got[len(got)-1] != '\n' {
		t.Fatalf("ND write = %q, want trailing newline", got)
	}

	readMsg, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !reflect.DeepEqual(msg, readMsg) {
		t.Errorf("expected %+v, got %+v", msg, readMsg)
	}
}

func TestNdFramerRejectsMultilineMessage(t *testing.T) {
	ctx := context.Background()
	framer := NewNdFramer()
	buf := bytes.NewBufferString(`{
		"jsonrpc": "2.0",
		"method": "test"
	}` + "\n")
	r, _ := framer.Frame(&mockRWC{Reader: buf, Writer: io.Discard})

	_, err := r.Read(ctx)
	if err == nil {
		t.Fatal("expected multiline message to be rejected")
	}
}

func TestNdFramerRequiresNewline(t *testing.T) {
	ctx := context.Background()
	framer := NewNdFramer()
	buf := bytes.NewBufferString(`{"jsonrpc":"2.0","method":"test"}`)
	r, _ := framer.Frame(&mockRWC{Reader: buf, Writer: io.Discard})

	_, err := r.Read(ctx)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("Read error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestHeaderFramer(t *testing.T) {
	ctx := context.Background()
	framer := NewHeaderFramer()
	buf := &bytes.Buffer{}
	rwc := &mockRWC{Reader: buf, Writer: buf}
	r, w := framer.Frame(rwc)

	msg, _ := NewNotification("test", nil)
	data, _ := EncodeMessage(msg)
	if err := w.Write(ctx, msg); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify header format in buffer
	expectedHeader := fmt.Sprintf("Content-Length: %v\r\n\r\n", len(data))
	if !bytes.HasPrefix(buf.Bytes(), []byte(expectedHeader)) {
		t.Errorf("buffer does not start with expected header, got: %q", buf.String())
	}

	readMsg, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !reflect.DeepEqual(msg, readMsg) {
		t.Errorf("expected %+v, got %+v", msg, readMsg)
	}
}

func TestHeaderFramerInvalidContentLength(t *testing.T) {
	ctx := context.Background()
	framer := NewHeaderFramer()
	invalidHeader := "Content-Length: abc\r\n\r\n{}"
	buf := bytes.NewBufferString(invalidHeader)
	rwc := &mockRWC{Reader: buf, Writer: io.Discard}
	r, _ := framer.Frame(rwc)

	_, err := r.Read(ctx)
	if err == nil {
		t.Error("expected error for invalid Content-Length, got nil")
	}
}

func TestHeaderFramerMultiple(t *testing.T) {
	ctx := context.Background()
	framer := NewHeaderFramer()
	buf := &bytes.Buffer{}
	rwc := &mockRWC{Reader: buf, Writer: buf}
	r, w := framer.Frame(rwc)

	msg1, _ := NewNotification("test1", nil)
	msg2, _ := NewNotification("test2", nil)

	if err := w.Write(ctx, msg1); err != nil {
		t.Fatalf("Write1 failed: %v", err)
	}
	if err := w.Write(ctx, msg2); err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}

	read1, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read1 failed: %v", err)
	}
	if !reflect.DeepEqual(msg1, read1) {
		t.Errorf("expected msg1 %+v, got %+v", msg1, read1)
	}

	read2, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read2 failed: %v", err)
	}
	if !reflect.DeepEqual(msg2, read2) {
		t.Errorf("expected msg2 %+v, got %+v", msg2, read2)
	}
}

func TestHeaderReaderEOF(t *testing.T) {
	ctx := context.Background()
	framer := NewHeaderFramer()
	buf := &bytes.Buffer{}
	rwc := &mockRWC{Reader: buf, Writer: io.Discard}
	r, _ := framer.Frame(rwc)

	_, err := r.Read(ctx)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
