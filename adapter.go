package frio

import (
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

func (a *Adapter) ReadAt(key string, off int64, n int64) ([]byte, int, error) {
	if bytes, ok := a.cache.Get(key); ok && bytes != nil {
		return bytes, len(bytes), nil
	}

	r, _, err := a.keyStreamer.StreamAt(key, off, n)
	if err != nil {
		return nil, 0, err
	}

	p, err := io.ReadAll(r)
	if p != nil && err == nil {
		a.cache.Add(key, p)
	}

	return p, len(p), nil
}

type Reader struct {
	a   *Adapter
	key string
	off int64
}

func (r *Reader) Read() ([]byte, int, error) {
	bt, n, err := r.a.ReadAt(r.key, r.off)
	if err != nil {
		return nil, 0, err
	}

	return bt, n, err
}

func (r *Reader) ReadAt(off int64) ([]byte, int, error) {
	bt, n, err := r.a.ReadAt(r.key, off)
	if err != nil {
		return nil, 0, err
	}

	return bt, n, nil
}

func (r *Reader) Seek(off int64, nWhence int) (int64, error) {
	coff := r.off
	switch nWhence {
	case io.SeekCurrent:
		coff += off
	case io.SeekStart:
		coff = off
	case io.SeekEnd:
		coff = off
	default:
		return 0, os.ErrInvalid
	}
	if coff < 0 {
		return r.off, os.ErrInvalid
	}
	r.off = coff
	return r.off, nil
}
