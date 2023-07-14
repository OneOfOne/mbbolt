package mbbolt

import (
	"archive/zip"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"go.oneofone.dev/genh"
	"go.oneofone.dev/oerrs"
	"go.oneofone.dev/otk"
)

func DefaultSegmentByKey(key string) uint64 {
	h := fnv.New64()
	io.WriteString(h, key)
	return h.Sum64()
}

// NewSegDB creates a new segmented database.
// SegDB uses msgpack by default.
// WARNING WARNING, if numSegments changes between calls, the keys will be out of sync
func NewSegDB(ctx context.Context, prefix, ext string, opts *Options, numSegments int) *SegDB {
	if numSegments < 1 {
		log.Panic("numSegments < 1")
	}

	seg := &SegDB{
		mdb: NewMultiDB(prefix, ext, opts),
		dbs: make([]*DB, numSegments),

		SegmentFn: DefaultSegmentByKey,
	}

	var wg sync.WaitGroup
	wg.Add(numSegments)
	for i := 0; i < numSegments; i++ {
		i, name := i, fmt.Sprintf("%06d", i)
		go func() {
			defer wg.Done()
			db := seg.mdb.MustGet(ctx, name, opts)
			if opts == nil || opts.MarshalFn == nil {
				db.SetMarshaler(genh.MarshalMsgpack, genh.UnmarshalMsgpack)
			}
			seg.dbs[i] = db
		}()
	}
	wg.Wait()
	return seg
}

func NewSegDBFromFile(ctx context.Context, fp, prefix, ext string, opts *Options) (_ *SegDB, err error) {
	var f *os.File
	if f, err = os.Open(fp); err != nil {
		return
	}
	defer f.Close()
	var segs int
	if err = otk.Unzip(f, prefix, func(fp string, f *zip.File) bool {
		if strings.HasSuffix(fp, ext) {
			segs++
			return true
		}
		return false
	}); err != nil {
		return
	}

	if segs == 0 {
		err = oerrs.String("no files found")
		return
	}

	return NewSegDB(ctx, prefix, ext, opts, segs), nil
}

type SegDB struct {
	SegmentFn func(key string) uint64

	mdb *MultiDB
	dbs []*DB
}

func (s *SegDB) Close() error {
	return s.mdb.Close()
}

func (s *SegDB) SetMarshaler(marshalFn MarshalFn, unmarshalFn UnmarshalFn) {
	for _, db := range s.dbs {
		db.SetMarshaler(marshalFn, unmarshalFn)
	}
}

func (s *SegDB) Get(bucket, key string, v any) error {
	return s.db(key).Get(bucket, key, v)
}

func (s *SegDB) ForEachBytes(bucket string, fn func(k, v []byte) error) error {
	for _, db := range s.dbs {
		if err := db.ForEachBytes(bucket, fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *SegDB) ForEachDB(fn func(db *DB) error) error {
	for _, db := range s.dbs {
		if err := fn(db); err != nil {
			return err
		}
	}
	return nil
}

func (s *SegDB) Put(bucket, key string, v any) error {
	return s.db(key).Put(bucket, key, v)
}

func (s *SegDB) Delete(bucket, key string) error {
	return s.db(key).Delete(bucket, key)
}

func (s *SegDB) SetNextIndex(bucket string, seq uint64) error {
	return s.dbs[0].SetNextIndex(bucket, seq)
}

func (s *SegDB) SetNextIndexByKey(bucket, key string, seq uint64) error {
	return s.db(key).SetNextIndex(bucket, seq)
}

func (s *SegDB) NextIndex(bucket string) (seq uint64, err error) {
	return s.dbs[0].NextIndex(bucket)
}

func (s *SegDB) NextIndexByKey(bucket, key string) (seq uint64, err error) {
	return s.db(key).NextIndex(bucket)
}

func (s *SegDB) CurrentIndex(bucket string) (idx uint64) {
	return s.dbs[0].CurrentIndex(bucket)
}

func (s *SegDB) CurrentIndexByKey(bucket, key string) (idx uint64) {
	return s.db(key).CurrentIndex(bucket)
}

func (s *SegDB) Buckets() []string {
	var set otk.Set
	for _, db := range s.dbs {
		set = set.Add(db.Buckets()...)
	}
	return set.SortedKeys()
}

func (s *SegDB) Backup(w io.Writer) (int64, error) {
	return s.mdb.Backup(w, nil)
}

func (s *SegDB) RestoreFromFile(ctx context.Context, fp string) error {
	return s.mdb.RestoreFromFile(ctx, fp)
}

func (s *SegDB) Restore(ctx context.Context, r io.ReaderAt) error {
	return s.mdb.Restore(ctx, r)
}

func (s *SegDB) UseBatch(v bool) (old bool) {
	for _, db := range s.dbs {
		old = db.UseBatch(v)
	}
	return
}

func SegDBUpdateOne[T any](s *SegDB, bucket, key string, fn func(v T) (T, error)) error {
	db := s.db(key)
	return db.Update(func(tx *Tx) (err error) {
		var v T
		tx.GetValue(bucket, key, &v)
		if v, err = fn(v); err != nil {
			return
		}
		return tx.PutValue(bucket, key, v)
	})
}

func (s *SegDB) db(key string) *DB {
	return s.dbs[s.SegmentFn(key)%uint64(len(s.dbs))]
}
