package main

import (
	"errors"
)

func get(s storage, k string) (string, error) {
	reader, err := createReader(s.path)
	if err != nil {
		return "", err
	}

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

func put(s storage, k string, v string) error {
	le := createLogEntry(k, []byte(v))
	err := writeLogEntry(s, le)
	if err != nil {
		return err
	}
	return nil
}
