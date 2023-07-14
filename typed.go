package mbbolt

import "context"

type TxBase interface {
	GetBytes(bucket, key string, clone bool) (out []byte)
	ForEachBytes(bucket string, fn func(k, v []byte) error) error
	PutBytes(bucket, key string, val []byte) error
	Delete(bucket, key string) error
	DeleteBucket(bucket string) error
}

var _ TxBase = (*Tx)(nil)

func OpenTDB[T any](ctx context.Context, path string, opts *Options) (db TypedDB[T], err error) {
	db.DB, err = Open(ctx, path, opts)
	return
}

func OpenMultiTDB[T any](ctx context.Context, m *MultiDB, path string, opts *Options) (db TypedDB[T], err error) {
	db.DB, err = m.Get(ctx, path, opts)
	return
}

func DBToTyped[T any](db *DB) TypedDB[T] { return TypedDB[T]{db} }

type TypedDB[T any] struct {
	*DB
}

func (db TypedDB[T]) ForEach(bucket string, fn func(key string, v T) error) error {
	return db.View(func(tx *Tx) error {
		ttx := TypedTx[T]{tx}
		return ttx.ForEach(bucket, fn)
	})
}

func (db TypedDB[T]) Get(bucket, key string) (v T, err error) {
	err = db.GetAny(bucket, key, &v, db.unmarshalFn)
	return
}

func (db TypedDB[T]) Put(bucket, key string, val T) error {
	return db.PutAny(bucket, key, val, db.marshalFn)
}

type TypedTx[T any] struct {
	*Tx
}

func (tx TypedTx[T]) ForEach(bucket string, fn func(key string, v T) error) error {
	return tx.ForEachBytes(bucket, func(k, v []byte) (err error) {
		var tv T
		if err = tx.db.unmarshalFn(v, &tv); err != nil {
			return err
		}
		return fn(string(k), tv)
	})
}

func (tx TypedTx[T]) Get(bucket, key string) (v T, err error) {
	err = tx.Tx.GetValue(bucket, key, &v)
	return
}

func (tx TypedTx[T]) Put(bucket, key string, v T) error {
	return tx.Tx.PutValue(bucket, key, v)
}

func (tx TypedTx[T]) MustGet(bucket, key string, def T) (v T) {
	if err := tx.Tx.getAny(true, bucket, key, &v, tx.db.unmarshalFn); err != nil {
		return def
	}
	return
}
