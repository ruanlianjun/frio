package file

import (
	"bytes"
	"context"
	"io"
	"os"
)

type Handler struct {
	ctx context.Context
}

func (h *Handler) StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error) {
	stat, err := os.Stat(key)
	if err != nil {
		return nil, 0, err
	}

	file, err := os.Open(key)
	if err != nil {
		return nil, 0, err
	}

	defer file.Close()

	var size int64

	_, err = file.Seek(off, io.SeekStart)
	if err != nil {
		return nil, 0, err
	}
	var buf bytes.Buffer
	n, err = io.Copy(&buf, file)
	if off == 0 {
		size = stat.Size()
	} else {
		size = n
	}

	return io.NopCloser(bytes.NewReader(buf.Bytes())), size, err
}
