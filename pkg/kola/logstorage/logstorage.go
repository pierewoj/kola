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

type ReadToken struct {
	reader *bufio.Reader
}

type ReadResult struct {
	LogEntry *LogEntry
	Token    *ReadToken
}

func CreateStorage(path string) (*Storage, error) {
	writer, err := createWriter(path)
	if err != nil {
		return nil, err
	}
	return &Storage{
		writer: writer,
		Path:   path,
	}, nil
}

func (s *Storage) ReadEntry(token *ReadToken) (*ReadResult, error) {
	reader, err := useReaderFromTokenOrCreate(s, token)
	if err != nil {
		return nil, err
	}
	keyLenBytes := make([]byte, 8)
	_, err = io.ReadFull(reader, keyLenBytes)
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
	le := LogEntry{
		keyLen: keyLen,
		valLen: valLen,
		Key:    keyBytes,
		Val:    valBytes,
	}
	return &ReadResult{
		LogEntry: &le,
		Token: &ReadToken{
			reader: reader,
		},
	}, nil
}

func (s *Storage) WriteLogEntry(le LogEntry) error {
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

func CreateLogEntry(k string, v []byte) LogEntry {
	kB := []byte(k)
	return LogEntry{
		keyLen: uint64(len(kB)),
		valLen: uint64(len(v)),
		Key:    kB,
		Val:    v,
	}
}

func useReaderFromTokenOrCreate(s *Storage, token *ReadToken) (*bufio.Reader, error) {
	if token == nil {
		return createReader(s)
	} else {
		return token.reader, nil
	}
}

func createReader(s *Storage) (*bufio.Reader, error) {
	f, err := os.OpenFile(s.Path, os.O_RDONLY, 0777)
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
