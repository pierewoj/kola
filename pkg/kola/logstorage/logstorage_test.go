package logstorage

import "testing"

func TestWriteAndRead(t *testing.T) {
	storage, err := CreateStorage("/tmp/test.txt")
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	le := CreateLogEntry("a", []byte("XD"))
	WriteLogEntry(*storage, le)

	reader, err := CreateReader(storage.Path)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	read, err := ReadEntry(reader)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	if string(read.Key) != "a" {
		t.Errorf("read.key=%s; want a", string(read.Key))
	}
	if string(read.Val) != "XD" {
		t.Errorf("read.key=%s; want XD", string(read.Val))
	}
	if read.keyLen != 1 {
		t.Errorf("read.keyLen=%d; want 1", read.keyLen)
	}
	if read.valLen != 2 {
		t.Errorf("read.valLen=%d; want 2", read.valLen)
	}
}
