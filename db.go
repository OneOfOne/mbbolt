package mbbolt

import (
	"encoding/json"
	"log"
	"math/big"
	"runtime"
	"time"
)

var (
	DefaultMarshalFn   = json.Marshal
	DefaultUnmarshalFn = json.Unmarshal
)

type DB struct {
	b           *BBoltDB
	marshalFn   MarshalFn
	unmarshalFn UnmarshalFn

	onClose func()
	slow    *slowUpdate
}

func (db *DB) SetMarshaler(marshalFn MarshalFn, unmarshalFn UnmarshalFn) {
	if marshalFn == nil || unmarshalFn == nil {
		log.Panic(" marshalFn == nil || unmarshalFn == nil")
	}
	db.marshalFn, db.unmarshalFn = marshalFn, unmarshalFn
}

func (db *DB) OnSlowUpdate(minDuration time.Duration, fn OnSlowUpdateFn) {
	if db.slow != nil {
		log.Panic("multiple calls")
	}
	if fn == nil || minDuration < time.Millisecond {
		log.Panic("fn == nil || minDuration < time.Millisecond")
	}
	db.slow = &slowUpdate{
		fn:  fn,
		min: minDuration,
	}
}

func (db *DB) GetBytes(bucket, key string) (out []byte, err error) {
	err = db.View(func(tx *Tx) error {
		out = tx.GetBytes(bucket, key, true)
		return nil
	})
	return
}

func (db *DB) PutBytes(bucket, key string, val []byte) error {
	return db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(unsafeBytes(bucket))
		if err != nil {
			return err
		}
		return b.Put(unsafeBytes(key), val)
	})
}

func (db *DB) Get(bucket, key string, out any) (err error) {
	return db.GetAny(bucket, key, out, db.unmarshalFn)
}

func (db *DB) Put(bucket, key string, val any) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}

func (db *DB) GetAny(bucket, key string, out any, unmarshalFn UnmarshalFn) error {
	return db.View(func(tx *Tx) error {
		return tx.GetAny(bucket, key, out, unmarshalFn)
	})
}

func (db *DB) PutAny(bucket, key string, val any, marshalFn MarshalFn) error {
	// duplicated code from tx.PutAny to keep the marshaling outside of the locks
	if b, ok := val.([]byte); ok {
		return db.PutBytes(bucket, key, b)
	}
	if marshalFn == nil {
		marshalFn = DefaultMarshalFn
	}
	b, err := marshalFn(val)
	if err != nil {
		return err
	}
	return db.PutBytes(bucket, key, b)
}

func (db *DB) View(fn func(*Tx) error) error {
	return db.b.View(db.getTxFn(fn))
}

func (db *DB) Update(fn func(*Tx) error) error {
	if db.slow != nil {
		return db.updateSlow(fn, db.slow)
	}

	return db.b.Update(db.getTxFn(fn))
}

func (db *DB) Batch(fn func(*Tx) error) error {
	return db.b.Batch(db.getTxFn(fn))
}

func (db *DB) CreateBucket(bucket string) error {
	bb := unsafeBytes(bucket)
	return db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists(bb)
		return err
	})
}

func (db *DB) CreateBucketWithIndex(bucket string, idx uint64) error {
	bb := unsafeBytes(bucket)
	return db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(bb)
		if err != nil {
			return err
		}
		return b.SetSequence(idx)
	})
}

func (db *DB) CreateBucketWithIndexBig(bucket string, idx *big.Int) error {
	if idx == nil {
		db.CreateBucketWithIndex(bucket, 0)
	}
	return db.CreateBucketWithIndex(bucket, idx.Uint64())
}

func (db *DB) Path() string  { return db.b.Path() }
func (db *DB) Raw() *BBoltDB { return db.b }

func (db *DB) Close() error {
	if db.onClose != nil {
		db.onClose()
	}
	return db.b.Close()
}

func (db *DB) updateSlow(fn func(*Tx) error, su *slowUpdate) (err error) {
	var pcs [6]uintptr

	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	start := time.Now()

	su.Lock()
	defer su.Unlock()

	err = db.b.Update(db.getTxFn(fn))
	if took := time.Since(start); took >= su.min {
		su.fn(frames, took)
	}

	return
}

func (db *DB) getTxFn(fn func(*Tx) error) func(tx *BBoltTx) error {
	return func(tx *BBoltTx) error { return fn(&Tx{tx, db}) }
}
