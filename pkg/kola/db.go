package main

import (
	"errors"
	"pierewoj/kola/logstorage"
)

func get(s logstorage.Storage, k string) (string, error) {
	reader, err := logstorage.CreateReader(s.Path)
	if err != nil {
		return "", err
	}

	found := false
	v := make([]byte, 1)
	for {
		le, err := logstorage.ReadEntry(reader)
		if err != nil {
			break
		}
		if string(le.Key) == k {
			found = true
			v = le.Val
		}
	}

	if found {
		return string(v), nil
	}
	return "", errors.New("key not found")
}

func put(s logstorage.Storage, k string, v string) error {
	le := logstorage.CreateLogEntry(k, []byte(v))
	err := logstorage.WriteLogEntry(s, le)
	if err != nil {
		return err
	}
	return nil
}
