package logstorage

import (
	"errors"
	"io"
	"testing"
)

func TestWriteAndRead(t *testing.T) {
	provider := createTestIo()
	storage, err := CreateStorage(&provider)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	le := CreateLogEntry("a", []byte("XD"))
	storage.WriteLogEntry(le)

	read, err := storage.ReadEntry(nil)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	if string(read.LogEntry.Key) != "a" {
		t.Errorf("read.key=%s; want a", string(read.LogEntry.Key))
	}
	if string(read.LogEntry.Val) != "XD" {
		t.Errorf("read.key=%s; want XD", string(read.LogEntry.Val))
	}
	if read.LogEntry.keyLen != 1 {
		t.Errorf("read.keyLen=%d; want 1", read.LogEntry.keyLen)
	}
	if read.LogEntry.valLen != 2 {
		t.Errorf("read.valLen=%d; want 2", read.LogEntry.valLen)
	}
}

type TestReader struct {
	sip   *StubIoProvider
	intex int
}

type TestWriter struct {
	sip *StubIoProvider
}

func createTestIo() StubIoProvider {
	return StubIoProvider{
		written: make([]byte, 0),
		buf:     make([]byte, 0),
	}
}

func (tr *TestReader) Read(p []byte) (n int, err error) {
	read := 0
	for i := 0; i < len(p); i++ {
		if tr.intex > len(tr.sip.written)-1 {
			return i, errors.New("reached end of input")
		}
		p[i] = tr.sip.written[tr.intex]
		tr.intex++
		read += 1
	}
	return read, nil
}

func (tw *TestWriter) Write(p []byte) (n int, err error) {
	tw.sip.buf = append(tw.sip.buf, p...)
	return len(p), nil
}

type StubIoProvider struct {
	written []byte
	buf     []byte
}

func (sip *StubIoProvider) GetReader() (io.Reader, error) {
	return &TestReader{
		sip:   sip,
		intex: 0,
	}, nil
}

func (sip *StubIoProvider) GetWriter() (io.Writer, error) {
	return &TestWriter{
		sip: sip,
	}, nil
}

func (sip *StubIoProvider) Flush() error {
	sip.written = append(sip.written, sip.buf...)
	sip.buf = make([]byte, 0)
	return nil
}
