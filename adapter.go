package frio

import (
	"io"
)

type KeyStreamerAt interface {
	StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error)
}
