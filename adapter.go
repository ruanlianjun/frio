package frio

import (
	"errors"
	"io"
	"os"
	"time"
)

type AdapterCache interface {
	Add(key string, data []byte) error
	Get(key string) ([]byte, bool)
	AddWithExpire(key string, data []byte, exp time.Duration) error
}

type KeyStreamerAt interface {
	StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error)
}

type Adapter struct {
	cache       AdapterCache
	keyStreamer KeyStreamerAt
}

type Options interface {
	adapterOpt(adapter *Adapter) error
}

type cache struct {
	numEntries int
}

func (c *cache) adapterOpt(adapter *Adapter) error {
	adapter.cache = NewLRUCache(c.numEntries)
	return nil
}

func WithDataCache(numEntries int) Options {
	return &cache{numEntries: numEntries}
}

func NewAdapter(keyStreamer KeyStreamerAt, options ...Options) *Adapter {
	ada := &Adapter{
		keyStreamer: keyStreamer,
	}
	for _, option := range options {
		option.adapterOpt(ada)
	}
	return ada
}

func (a *Adapter) Reader(key string) *Reader {
	return &Reader{
		a:   a,
		key: key,
		off: 0,
	}
}

func (a *Adapter) ReadAt(key string, p []byte, off int64) (int, error) {
	if bytes, ok := a.cache.Get(key); ok {
		p = bytes
		return len(bytes), nil
	}

	r, _, err := a.keyStreamer.StreamAt(key, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	n, err := io.ReadFull(r, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	if p != nil && err == nil {
		a.cache.Add(key, p)
	}
	return n, nil
}

type Reader struct {
	a    *Adapter
	key  string
	size int64
	off  int64
}

func (r *Reader) Read(buf []byte) (int, error) {
	if bt, ok := r.a.cache.Get(r.key); ok && bt != nil {
		buf = bt
		return len(bt), nil
	}

	if r.off >= r.size {
		return 0, io.EOF
	}
	n, err := r.a.ReadAt(r.key, buf, r.off)
	r.off += int64(n)

	if buf != nil {
		r.a.cache.Add(r.key, buf)
	}

	return n, err
}

func (r *Reader) ReadAt(buf []byte, off int64) (int, error) {
	if bt, ok := r.a.cache.Get(r.key); ok && bt != nil {
		buf = bt
		return len(bt), nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	if buf != nil {
		r.a.cache.Add(r.key, buf)
	}
	return r.a.ReadAt(r.key, buf, off)
}

func (r *Reader) Seek(off int64, nWhence int) (int64, error) {
	coff := r.off
	switch nWhence {
	case io.SeekCurrent:
		coff += off
	case io.SeekStart:
		coff = off
	case io.SeekEnd:
		coff = r.size + off
	default:
		return 0, os.ErrInvalid
	}
	if coff < 0 {
		return r.off, os.ErrInvalid
	}
	r.off = coff
	return r.off, nil
}
