package logstorage

import "testing"

func TestWriteAndRead(t *testing.T) {
	storage, err := CreateStorage("/tmp/test.txt")
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
