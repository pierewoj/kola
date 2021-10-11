package main

import "testing"

func TestWriteAndRead(t *testing.T) {
	storage, err := createStorage("/tmp/test.txt")
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	le := createLogEntry("a", []byte("XD"))
	writeLogEntry(*storage, le)

	reader, err := createReader(storage.path)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	read, err := readEntry(reader)
	if err != nil {
		t.Errorf("err=%s; want nil", err)
		return
	}
	if string(read.key) != "a" {
		t.Errorf("read.key=%s; want a", string(read.key))
	}
	if string(read.val) != "XD" {
		t.Errorf("read.key=%s; want XD", string(read.val))
	}
	if read.keyLen != 1 {
		t.Errorf("read.keyLen=%d; want 1", read.keyLen)
	}
	if read.valLen != 2 {
		t.Errorf("read.valLen=%d; want 2", read.valLen)
	}
}
