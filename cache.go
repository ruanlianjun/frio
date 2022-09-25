package frio

import (
	"math/rand"
	"strings"
	"time"
	"unsafe"

	"github.com/bluele/gcache"
)

type LRUCache struct {
	c      gcache.Cache
	random string
}

var _ AdapterCache = &LRUCache{}

func NewLRUCache(numEntries int) *LRUCache {
	cache := gcache.New(numEntries).LRU().Build()
	return &LRUCache{c: cache, random: randString(15)}
}

func (L *LRUCache) Add(key string, data []byte) error {
	return L.c.Set(setKey(key, L.random), data)
}

func (L *LRUCache) Get(key string) ([]byte, bool) {
	d, err := L.c.Get(setKey(key, L.random))
	if err != nil {
		return nil, false
	}
	return d.([]byte), true
}

func (L *LRUCache) AddWithExpire(key string, data []byte, exp time.Duration) error {
	return L.c.SetWithExpire(key, data, exp)
}

func setKey(key ...string) string {
	s := strings.Builder{}
	s.Grow(len(key))
	for i := range key {
		s.WriteString(key[i])
	}
	return s.String()
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

func randString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
