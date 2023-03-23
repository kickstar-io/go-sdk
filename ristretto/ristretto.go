package ristretto

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/ristretto"
)

var ErrNotFound = errors.New("ristretto: not found")

//Ristretto ...
type Ristretto struct {
	inc *ristretto.Cache
}

var ristrettoCache Ristretto

func (r *Ristretto) Instance() *ristretto.Cache { return r.inc }

//GetInc ...
func GetInc() *Ristretto {
	if ristrettoCache == (Ristretto{}) {
		ristrettoCache.constructor()
		return &ristrettoCache
	}

	return &ristrettoCache
}

func (r *Ristretto) constructor() {
	var err error
	r.inc, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,           // number of keys to track frequency of (10M).
		MaxCost:     (1 << 30) * 1, // maximum cost of cache (1GB).
		BufferItems: 64,            // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
}

func (r *Ristretto) String() string { return "ristretto" }
func (r *Ristretto) Set(key string, data interface{}) (err error) {
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return err
	}

	r.inc.SetWithTTL(key, jsonStr, 1, 0)
	return
}

func (r *Ristretto) SetWithTTL(key string, data interface{}, ttl int64) (err error) {
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return err
	}

	r.inc.SetWithTTL(key, jsonStr, 1, time.Duration(ttl)*time.Second)
	return
}

func (r *Ristretto) Get(key string, value interface{}) (data []byte, err error) {
	val, has := r.inc.Get(key)
	if !has {
		err = ErrNotFound
		return
	}
	switch d := val.(type) {
	case string:
		data = []byte(d)
	case []byte:
		data = d
	default:
		data, err = json.Marshal(d)
	}

	err = json.Unmarshal(data, &value)
	if err != nil {
		return nil, err
	}
	return
}

func (r *Ristretto) Delete(key string) (err error) { r.inc.Del(key); return }
func (r *Ristretto) Reset() (err error)            { r.inc.Clear(); return }
