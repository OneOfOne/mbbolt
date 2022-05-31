package genbolt

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/bbolt"
)

// bbolt type aliases
type (
	RawDB   = bbolt.DB
	Bucket  = bbolt.Bucket
	Cursor  = bbolt.Cursor
	TxStats = bbolt.TxStats
	Options = bbolt.Options

	RawTx = bbolt.Tx

	OnSlowUpdateFn func(callers *runtime.Frames, took time.Duration)
)

var DefaultOptions = Options{
	Timeout:        time.Second, // don't block indefinitely if the db isn't closed
	NoFreelistSync: true,        // improves write performance, slow load if the db isn't closed cleanly
	NoGrowSync:     false,
	FreelistType:   bbolt.FreelistMapType,

	// syscall.MAP_POPULATE on linux 2.6.23+ does sequential read-ahead
	// which can speed up entire-database read with boltdb.
	MmapFlags: syscall.MAP_POPULATE,
}

var all struct {
	MultiDB
	mdbs struct {
		sync.Mutex
		dbs []*MultiDB
	}
}

func Open(path string, opts *Options) (*DB, error) {
	return all.Get(path, opts)
}

func MustOpen(path string, opts *Options) *DB {
	return all.MustGet(path, opts)
}

func CloseAll() error {
	var errs []string
	if err := all.Close(); err != nil {
		errs = append(errs, err.Error())
	}

	all.mdbs.Lock()
	defer all.mdbs.Unlock()

	for _, mdb := range all.mdbs.dbs {
		if err := mdb.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	return errors.New(strings.Join(errs, ", "))
}

func NewMultiDB(prefix, ext string, opts *Options) *MultiDB {
	mdb := &MultiDB{opts: opts, prefix: prefix, ext: ext}
	all.mdbs.Lock()
	all.mdbs.dbs = append(all.mdbs.dbs, mdb)
	all.mdbs.Unlock()
	return mdb
}

type MultiDB struct {
	mux    sync.RWMutex
	m      map[string]*DB
	opts   *Options
	prefix string
	ext    string
}

func (mdb *MultiDB) MustGet(name string, opts *Options) *DB {
	db, err := mdb.Get(name, opts)
	if err != nil {
		log.Panicf("MustGet (%s): %v", name, err)
	}
	return db
}

func (mdb *MultiDB) Get(name string, opts *Options) (db *DB, err error) {
	fp := mdb.getPath(name)

	mdb.mux.RLock()
	if db = mdb.m[name]; db != nil {
		mdb.mux.RUnlock()
		return
	}
	mdb.mux.RUnlock()

	mdb.mux.Lock()
	defer mdb.mux.Unlock()

	// race check
	if db = mdb.m[name]; db != nil {
		return
	}

	if opts == nil {
		opts = mdb.opts
	}

	if opts == nil {
		opts = &DefaultOptions
	}

	var bdb *RawDB
	if bdb, err = bbolt.Open(fp, 0o600, opts); err != nil {
		return
	}

	db = &DB{
		RawDB:       bdb,
		marshalFn:   DefaultMarshalFn,
		unmarshalFn: DefaultUnmarshalFn,
	}

	if mdb.m == nil {
		mdb.m = map[string]*DB{}
	}

	mdb.m[name] = db

	return
}

func (mdb *MultiDB) CloseDB(name string) (err error) {
	mdb.mux.Lock()
	defer mdb.mux.Unlock()
	if db := mdb.m[name]; db != nil {
		err = db.Close()
		delete(mdb.m, name)
	}
	return
}

func (mdb *MultiDB) Close() error {
	mdb.mux.Lock()
	defer mdb.mux.Unlock()
	var buf strings.Builder
	for k, db := range mdb.m {
		if err := db.Close(); err != nil {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s: %v", k, db)
		}
		delete(mdb.m, k)
	}
	if buf.Len() > 0 {
		return errors.New(buf.String())
	}
	return nil
}

func (mdb *MultiDB) getPath(name string) string {
	if mdb.prefix != "" {
		name = filepath.Join(mdb.prefix, name)
	}

	if mdb.ext != "" {
		name += mdb.ext
	}

	return name
}
