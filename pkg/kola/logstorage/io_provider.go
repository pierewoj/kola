package logstorage

import (
	"bufio"
	"io"
	"os"
)

type IoProvider interface {
	GetReader() (io.Reader, error)
	GetWriter() (io.Writer, error)
	Flush() error
}

type RealIoProvider struct {
	path   string
	writer *bufio.Writer
}

func (riop *RealIoProvider) GetReader() (io.Reader, error) {
	f, err := os.OpenFile(riop.path, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(f), nil
}

func (riop *RealIoProvider) GetWriter() (io.Writer, error) {
	return riop.writer, nil
}

func (riop *RealIoProvider) Flush() error {
	return riop.writer.Flush()
}

func CreateIoPriovider(path string) (*RealIoProvider, error) {
	writer, err := createWriter(path)
	if err != nil {
		return nil, err
	}
	return &RealIoProvider{
		path:   path,
		writer: writer,
	}, nil
}

func createWriter(path string) (*bufio.Writer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	return bufio.NewWriter(f), nil
}
