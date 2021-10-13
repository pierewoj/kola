package logstorage

import (
	"encoding/binary"
	"io"
)

type Storage struct {
	writer     io.Writer
	ioProvider IoProvider
}

type LogEntry struct {
	keyLen uint64
	valLen uint64
	Key    []byte
	Val    []byte
}

type ReadToken struct {
	reader io.Reader
}

type ReadResult struct {
	LogEntry *LogEntry
	Token    *ReadToken
}

func CreateStorage(ioProvider IoProvider) (*Storage, error) {
	writer, err := ioProvider.GetWriter()
	if err != nil {
		return nil, err
	}
	return &Storage{
		writer:     writer,
		ioProvider: ioProvider,
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

	return s.ioProvider.Flush()
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

func useReaderFromTokenOrCreate(s *Storage, token *ReadToken) (io.Reader, error) {
	if token == nil {
		return s.ioProvider.GetReader()
	} else {
		return token.reader, nil
	}
}
