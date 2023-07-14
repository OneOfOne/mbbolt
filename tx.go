package mbbolt

import (
	"log"
	"math/big"
)

type (
	MarshalFn   = func(any) ([]byte, error)
	UnmarshalFn = func([]byte, any) error
)

type Tx struct {
	*BBoltTx
	db *DB
}

func (tx *Tx) CreateBucketIfNotExists(bucket string) (*Bucket, error) {
	return tx.BBoltTx.CreateBucketIfNotExists(unsafeBytes(bucket))
}

func (tx *Tx) Bucket(bucket string) *Bucket {
	return tx.BBoltTx.Bucket(unsafeBytes(bucket))
}

func (tx *Tx) MustBucket(bucket string) *Bucket {
	if b := tx.BBoltTx.Bucket(unsafeBytes(bucket)); b != nil {
		return b
	}

	b, err := tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		log.Panicf("%s: %v", bucket, err)
	}
	return b
}

func (tx *Tx) GetBytes(bucket, key string, clone bool) (out []byte) {
	if b := tx.Bucket(bucket); b != nil {
		if out = b.Get(unsafeBytes(key)); clone {
			out = append([]byte(nil), out...)
		}
		return
	}
	return
}

func (tx *Tx) PutBytes(bucket, key string, val []byte) error {
	if err := tx.db.ctx.Err(); err != nil {
		return err
	}
	if b := tx.MustBucket(bucket); b != nil {
		return b.Put(unsafeBytes(key), val)
	}
	return ErrBucketNotFound
}

func (tx *Tx) GetValue(bucket, key string, out any) error {
	return tx.GetAny(bucket, key, out, tx.db.unmarshalFn)
}

func (tx *Tx) PutValue(bucket, key string, val any) error {
	return tx.PutAny(bucket, key, val, tx.db.marshalFn)
}

func (tx *Tx) Delete(bucket, key string) error {
	if b := tx.Bucket(bucket); b != nil {
		return b.Delete(unsafeBytes(key))
	}
	return ErrBucketNotFound
}

func (tx *Tx) DeleteBucket(bucket string) error {
	return tx.BBoltTx.DeleteBucket(unsafeBytes(bucket))
}

func (tx *Tx) GetAny(bucket, key string, out any, unmarshalFn UnmarshalFn) error {
	return tx.getAny(false, bucket, key, out, unmarshalFn)
}

func (tx *Tx) getAny(createBucket bool, bucket, key string, out any, unmarshalFn UnmarshalFn) (err error) {
	var b *Bucket
	if createBucket {
		if b, err = tx.CreateBucketIfNotExists(bucket); err != nil {
			return
		}
	} else {
		if b = tx.Bucket(bucket); b == nil {
			return ErrBucketNotFound
		}
	}

	if err = tx.db.ctx.Err(); err != nil {
		return
	}

	val := b.Get(unsafeBytes(key))
	switch out := out.(type) {
	case *[]byte:
		*out = append([]byte(nil), val...)
	// case *string:
	// 	*out = string(val)
	default:
		if unmarshalFn == nil {
			unmarshalFn = DefaultUnmarshalFn
		}
		return unmarshalFn(val, out)
	}
	return nil
}

func (tx *Tx) PutAny(bucket, key string, val any, marshalFn MarshalFn) error {
	switch val := val.(type) {
	case []byte:
		return tx.PutBytes(bucket, key, val)
	// case string:
	// 	return tx.PutBytes(bucket, key, unsafeBytes(val))
	default:
		if marshalFn == nil {
			marshalFn = DefaultMarshalFn
		}
		b, err := marshalFn(val)
		if err != nil {
			return err
		}
		return tx.PutBytes(bucket, key, b)
	}
}

func (tx *Tx) ForEachBytes(bucket string, fn func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil {
		return b.ForEach(func(k, v []byte) error {
			if err := tx.db.ctx.Err(); err != nil {
				return err
			}
			return fn(k, v)
		})
	}
	return ErrBucketNotFound
}

func (tx *Tx) Range(bucket string, start []byte, fn func(cursor *Cursor, k, v []byte) error, forward bool) (err error) {
	c := tx.Bucket(bucket).Cursor()
	if forward {
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if err = fn(c, k, v); err != nil {
				return
			}
		}
	} else {
		for k, v := c.Seek(start); k != nil; k, v = c.Prev() {
			if err = fn(c, k, v); err != nil {
				return
			}
		}
	}
	return
}

// ForEachUpdate passes a func to the loop func to allow you to set values inside the loop,
// this is a workaround seting values inside a foreach loop which isn't allowed.
func (tx *Tx) ForEachUpdate(bucket string, fn func(k, v []byte, setValue func(k, nv []byte)) (err error)) (err error) {
	var updateTable map[string][]byte
	b := tx.Bucket(bucket)

	setValue := func(k, v []byte) {
		if updateTable == nil {
			updateTable = map[string][]byte{}
		}
		updateTable[string(k)] = v
	}

	if err = b.ForEach(func(k, v []byte) error {
		if err := tx.db.ctx.Err(); err != nil {
			return err
		}
		return fn(k, v, setValue)
	}); err != nil {
		return
	}

	for k, v := range updateTable {
		if err = tx.db.ctx.Err(); err != nil {
			return
		}
		kb := unsafeBytes(k)
		if v == nil {
			err = b.Delete(kb)
		} else {
			err = b.Put(kb, v)
		}
		if err != nil {
			return
		}
	}

	return
}

func (tx *Tx) SetNextIndex(bucket string, idx uint64) error {
	return tx.MustBucket(bucket).SetSequence(idx)
}

func (tx *Tx) NextIndex(bucket string) (uint64, error) {
	return tx.MustBucket(bucket).NextSequence()
}

func (tx *Tx) NextIndexBig(bucket string) (*big.Int, error) {
	u, err := tx.NextIndex(bucket)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetUint64(u), nil
}

func GetTxAny[T any](tx *Tx, bucket, key string, unmarshalFn UnmarshalFn) (out T, err error) {
	if unmarshalFn == nil {
		unmarshalFn = DefaultUnmarshalFn
	}
	err = tx.GetAny(bucket, key, &out, unmarshalFn)
	return
}

func GetAny[T any](db *DB, bucket, key string, unmarshalFn UnmarshalFn) (out T, err error) {
	err = db.View(func(tx *Tx) error {
		out, err = GetTxAny[T](tx, bucket, key, unmarshalFn)
		return err
	})
	return
}

func ForEachTx[T any](tx *Tx, bucket string, fn func(key []byte, val T) error, filterFn func(k, v []byte) bool, unmarshalFn UnmarshalFn) error {
	b := tx.Bucket(bucket)
	if b == nil {
		return ErrBucketNotFound
	}

	if unmarshalFn == nil {
		unmarshalFn = DefaultUnmarshalFn
	}

	if filterFn == nil {
		filterFn = filterOk
	}
	return b.ForEach(func(k, v []byte) (err error) {
		if !filterFn(k, v) {
			return
		}
		var val T
		if err = unmarshalFn(v, &val); err != nil {
			return
		}
		return fn(k, val)
	})
}

// func getTx(tx *Tx, bucket string, id string, clone bool) (out []byte) {
// 	out = tx.Bucket(bucket).Get(unsafeBytes(id))
// 	if clone {
// 		out = append([]byte(nil), out...)
// 	}
// 	return
// }

// func putTx(tx *Tx, bucket string, id string, value []byte) error {
// 	return tx.Bucket(bucket).Put(unsafeBytes(id), value)
// }

func filterOk(_, _ []byte) bool { return true }
