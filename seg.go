package mbbolt

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"

	"go.oneofone.dev/genh"
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
func NewSegDB(prefix, ext string, opts *Options, numSegments int) *SegDB {
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
			db := seg.mdb.MustGet(name, opts)
			if opts == nil || opts.MarshalFn == nil {
				db.SetMarshaler(genh.MarshalMsgpack, genh.UnmarshalMsgpack)
			}
			seg.dbs[i] = db
		}()
	}
	wg.Wait()
	return seg
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

func (s *SegDB) Put(bucket, key string, v any) error {
	return s.db(key).Put(bucket, key, v)
}

func (s *SegDB) Delete(bucket, key string) error {
	return s.db(key).Delete(bucket, key)
}

func (s *SegDB) SetNextIndex(bucket string, seq uint64) error {
	return s.dbs[0].SetNextIndex(bucket, seq)
}

func (s *SegDB) NextIndex(bucket string) (seq uint64, err error) {
	return s.dbs[0].NextIndex(bucket)
}

func (s *SegDB) CurrentIndex(bucket string) (idx uint64) {
	s.dbs[0].View(func(tx *Tx) error {
		if b := tx.Bucket(bucket); b != nil {
			idx = b.Sequence()
		}
		return nil
	})
	return
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

func (s *SegDB) UseBatch(v bool) (old bool) {
	for _, db := range s.dbs {
		old = db.UseBatch(v)
	}
	return
}

func (s *SegDB) db(key string) *DB {
	return s.dbs[s.SegmentFn(key)%uint64(len(s.dbs))]
}
