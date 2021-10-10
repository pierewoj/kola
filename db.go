package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type storage struct {
	writer *bufio.Writer
}

type logEntry struct {
	keyLen uint64
	valLen uint64
	key    []byte
	val    []byte
}

func get(s storage, k string) (string, error) {
	f, err := os.OpenFile("/tmp/log.txt", os.O_RDONLY, 0777)
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(f)

	found := false
	v := make([]byte, 1)
	for {
		le, err := readEntry(reader)
		if err != nil {
			break
		}
		if string(le.key) == k {
			found = true
			v = le.val
		}
	}

	if found {
		return string(v), nil
	}
	return "", errors.New("Key not found")
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

func put(s storage, k string, v string) error {
	le := createLogEntry(k, v)
	err := writeLogEntry(s, le)
	if err != nil {
		return err
	}
	return nil
}

func createStorage() (*storage, error) {
	f, err := os.OpenFile("/tmp/log.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	return &storage{
		writer: bufio.NewWriter(f),
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
	println(fmt.Sprint(le.keyLen) + fmt.Sprint(le.valLen) + string(le.key) + string(le.val))

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
