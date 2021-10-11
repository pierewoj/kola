package logstorage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type Storage struct {
	writer *bufio.Writer
	Path   string
}

type LogEntry struct {
	keyLen uint64
	valLen uint64
	Key    []byte
	Val    []byte
}

func ReadEntry(reader *bufio.Reader) (*LogEntry, error) {
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
	return &LogEntry{
		keyLen: keyLen,
		valLen: valLen,
		Key:    keyBytes,
		Val:    valBytes,
	}, nil
}

func CreateStorage(path string) (*Storage, error) {
	writer, err := CreateWriter(path)
	if err != nil {
		return nil, err
	}
	return &Storage{
		writer: writer,
		Path:   path,
	}, nil
}

func CreateLogEntry(k string, v []byte) LogEntry {
	kB := []byte(k)
	return LogEntry{
		keyLen: uint64(len(kB)),
		valLen: uint64(len(v)),
		Key:    kB,
		Val:    v,
	}
}

func WriteLogEntry(s Storage, le LogEntry) error {
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
	s.writer.Write(le.Key)
	s.writer.Write(le.Val)

	return s.writer.Flush()
}

func CreateReader(path string) (*bufio.Reader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(f), nil
}

func CreateWriter(path string) (*bufio.Writer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewWriter(f), nil
}
