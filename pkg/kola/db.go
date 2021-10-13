package main

import (
	"errors"
	"pierewoj/kola/logstorage"
)

func get(s logstorage.Storage, k string) (string, error) {
	found := false
	v := make([]byte, 1)
	var token *logstorage.ReadToken = nil
	for {
		readResult, err := s.ReadEntry(token)
		if err != nil {
			break
		}
		if string(readResult.LogEntry.Key) == k {
			found = true
			v = readResult.LogEntry.Val
		}
		token = readResult.Token
	}

	if found {
		return string(v), nil
	}
	return "", errors.New("key not found")
}

func put(s logstorage.Storage, k string, v string) error {
	le := logstorage.CreateLogEntry(k, []byte(v))
	err := s.WriteLogEntry(le)
	if err != nil {
		return err
	}
	return nil
}
