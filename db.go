package mbbolt

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"go.oneofone.dev/genh"
	"go.oneofone.dev/oerrs"
)

var (
	DefaultMarshalFn   = json.Marshal
	DefaultUnmarshalFn = json.Unmarshal
	ErrBucketNotFound  = bbolt.ErrBucketNotFound
)

type DB struct {
	ctx context.Context
	b   *BBoltDB

	marshalFn   MarshalFn
	unmarshalFn UnmarshalFn

	onClose func()
	slow    atomic.Pointer[slowUpdate]

	useBatch genh.AtomicBool
}

func (db *DB) SetMarshaler(marshalFn MarshalFn, unmarshalFn UnmarshalFn) {
	if marshalFn == nil || unmarshalFn == nil {
		log.Panic("marshalFn == nil || unmarshalFn == nil")
	}
	db.marshalFn, db.unmarshalFn = marshalFn, unmarshalFn
}

func (db *DB) OnSlowUpdate(minDuration time.Duration, fn OnSlowUpdateFn) {
	if fn == nil || minDuration < time.Millisecond {
		log.Panic("fn == nil || minDuration < time.Millisecond")
	}
	db.slow.Store(&slowUpdate{
		fn:  fn,
		min: minDuration,
	})
}

func (db *DB) GetBytes(bucket, key string) (out []byte, err error) {
	err = db.View(func(tx *Tx) error {
		out = tx.GetBytes(bucket, key, true)
		return nil
	})
	return
}

func (db *DB) ForEachBytes(bucket string, fn func(k, v []byte) error) (err error) {
	return db.View(func(tx *Tx) error {
		return tx.ForEachBytes(bucket, fn)
	})
}

func (db *DB) PutBytes(bucket, key string, val []byte) error {
	fn := func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return b.Put(unsafeBytes(key), val)
	}

	if !db.useBatch.Load() {
		return db.Update(fn)
	}
	return db.Batch(fn)
}

func (db *DB) Get(bucket, key string, out any) (err error) {
	return db.GetAny(bucket, key, out, db.unmarshalFn)
}

func (db *DB) Put(bucket, key string, val any) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}

func (db *DB) Delete(bucket, key string) error {
	return db.Update(func(tx *Tx) error {
		return tx.Delete(bucket, key)
	})
}

func (db *DB) GetAny(bucket, key string, out any, unmarshalFn UnmarshalFn) error {
	return db.View(func(tx *Tx) error {
		return tx.GetAny(bucket, key, out, unmarshalFn)
	})
}

func (db *DB) Buckets() (out []string) {
	db.View(func(tx *Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			out = append(out, string(name))
			return nil
		})
	})
	out = genh.Clip(out)
	return
}

func (db *DB) PutAny(bucket, key string, val any, marshalFn MarshalFn) error {
	// duplicated code from tx.PutAny to keep the marshaling outside of the locks

	switch val := val.(type) {
	case []byte:
		return db.PutBytes(bucket, key, val)
	// case string:
	// 	return db.PutBytes(bucket, key, unsafeBytes(val))
	default:
		if marshalFn == nil {
			marshalFn = DefaultMarshalFn
		}
		b, err := marshalFn(val)
		if err != nil {
			return err
		}
		return db.PutBytes(bucket, key, b)
	}
}

func (db *DB) SetNextIndex(bucket string, seq uint64) error {
	return db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return b.SetSequence(seq)
	})
}

func (db *DB) NextIndex(bucket string) (idx uint64, err error) {
	err = db.Update(func(tx *Tx) error {
		var b *Bucket
		if b, err = tx.CreateBucketIfNotExists(bucket); err != nil {
			return err
		}
		idx, err = b.NextSequence()
		return err
	})
	return
}

func (db *DB) CurrentIndex(bucket string) (idx uint64) {
	db.View(func(tx *Tx) error {
		if b := tx.Bucket(bucket); b != nil {
			idx = b.Sequence()
		}
		return nil
	})
	return
}

func (db *DB) View(fn func(*Tx) error) error {
	return db.b.View(db.getTxFn(fn))
}

func (db *DB) Update(fn func(*Tx) error) error {
	if slow := db.slow.Load(); slow != nil {
		return db.updateSlow(fn, slow, false)
	}

	return db.b.Update(db.getTxFn(fn))
}

func (db *DB) Batch(fn func(*Tx) error) error {
	if slow := db.slow.Load(); slow != nil {
		return db.updateSlow(fn, slow, true)
	}
	return db.b.Batch(db.getTxFn(fn))
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.b.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &Tx{tx, db}, nil
}

func (db *DB) CreateBucket(bucket string) error {
	return db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})
}

func (db *DB) CreateBucketWithIndex(bucket string, idx uint64) error {
	return db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
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

func (db *DB) BackupToFile(fp string) (n int64, err error) {
	var f *os.File
	if f, err = os.Create(fp); err != nil {
		return
	}
	buf := getBuf(f)
	defer func() {
		putBufAndFlush(buf)
		if err2 := f.Close(); err2 != nil {
			err = oerrs.Join(err, err2)
		}
	}()
	return db.Backup(buf)
}

func (db *DB) Backup(w io.Writer) (n int64, err error) {
	db.b.View(func(tx *BBoltTx) error {
		n, err = tx.WriteTo(w)
		return err
	})
	return
}

func (db *DB) Path() string  { return db.b.Path() }
func (db *DB) Raw() *BBoltDB { return db.b }

func (db *DB) Close() error {
	if db.onClose != nil {
		db.onClose()
	}
	return db.b.Close()
}

func (db *DB) UseBatch(v bool) (old bool) {
	return db.useBatch.Swap(v)
}

func (db *DB) updateSlow(fn func(*Tx) error, su *slowUpdate, batch bool) (err error) {
	var pcs [6]uintptr

	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	start := time.Now()

	su.Lock()
	defer su.Unlock()

	if batch {
		err = db.b.Batch(db.getTxFn(fn))
	} else {
		err = db.b.Update(db.getTxFn(fn))
	}

	if took := time.Since(start); took >= su.min {
		su.fn(frames, took)
	}

	return
}

func (db *DB) getTxFn(fn func(*Tx) error) func(tx *BBoltTx) error {
	return func(tx *BBoltTx) error {
		if err := db.ctx.Err(); err != nil {
			return err
		}
		return fn(&Tx{tx, db})
	}
}
