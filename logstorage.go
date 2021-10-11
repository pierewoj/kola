package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type storage struct {
	writer *bufio.Writer
	path   string
}

type logEntry struct {
	keyLen uint64
	valLen uint64
	key    []byte
	val    []byte
}

func readEntry(reader *bufio.Reader) (*logEntry, error) {
	keyLenBytes := make([]byte, 8)
	_, err := io.ReadFull(reader, keyLenBytes)
	if err != nil {
		return nil, err
	}
	keyLen := binary.LittleEndian.Uint64(keyLenBytes)
	valLenBytes := make([]byte, 8)
	_, err = io.ReadFull(reader, valLenBytes)
	if err != nil {
		return nil, err
	}
	valLen := binary.LittleEndian.Uint64(valLenBytes)
	keyBytes := make([]byte, keyLen)
	_, err = io.ReadFull(reader, keyBytes)
	if err != nil {
		return nil, err
	}
	valBytes := make([]byte, valLen)
	_, err = io.ReadFull(reader, valBytes)
	if err != nil {
		return nil, err
	}
	return &logEntry{
		keyLen: keyLen,
		valLen: valLen,
		key:    keyBytes,
		val:    valBytes,
	}, nil
}

func createStorage(path string) (*storage, error) {
	writer, err := createWriter(path)
	if err != nil {
		return nil, err
	}
	return &storage{
		writer: writer,
		path:   path,
	}, nil
}

func createLogEntry(k string, v string) logEntry {
	kB := []byte(k)
	vB := []byte(v)
	return logEntry{
		keyLen: uint64(len(kB)),
		valLen: uint64(len(vB)),
		key:    kB,
		val:    vB,
	}
}

func writeLogEntry(s storage, le logEntry) error {
	// Key len
	{
		keyLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyLenBytes, uint64(le.keyLen))
		s.writer.Write(keyLenBytes)
	}

	// Val len
	{
		valLenBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valLenBytes, uint64(le.valLen))
		s.writer.Write(valLenBytes)
	}

	// Key, val
	s.writer.Write(le.key)
	s.writer.Write(le.val)

	return s.writer.Flush()
}

func createReader(path string) (*bufio.Reader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(f), nil
}

func createWriter(path string) (*bufio.Writer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewWriter(f), nil
}
