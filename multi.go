package mbbolt

import (
	"errors"
	"fmt"
	"log"
	"os"
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
	BBoltDB = bbolt.DB
	BBoltTx = bbolt.Tx

	Bucket  = bbolt.Bucket
	Cursor  = bbolt.Cursor
	TxStats = bbolt.TxStats

	OnSlowUpdateFn func(callers *runtime.Frames, took time.Duration)
)

type Options struct {
	// OpenFile is used to open files. It defaults to os.OpenFile. This option
	// is useful for writing hermetic tests.
	OpenFile func(string, int, os.FileMode) (*os.File, error)

	// InitDB gets called on initial db open
	InitDB func(db *DB) error

	// FreelistType sets the backend freelist type. There are two options. Array which is simple but endures
	// dramatic performance degradation if database is large and framentation in freelist is common.
	// The alternative one is using hashmap, it is faster in almost all circumstances
	// but it doesn't guarantee that it offers the smallest page id available. In normal case it is safe.
	// The default type is array
	FreelistType bbolt.FreelistType

	// InitialBuckets will create the given slice of buckets on initial db open
	InitialBuckets []string

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int

	// PageSize overrides the default OS page size.
	PageSize int

	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Do not sync freelist to disk. This improves the database write performance
	// under normal operation, but requires a full database re-sync during recovery.
	NoFreelistSync bool
	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// NoSync sets the initial value of DB.NoSync. Normally this can just be
	// set directly on the DB itself when returned from Open(), but this option
	// is useful in APIs which expose Options but not the underlying DB.
	NoSync bool

	// Mlock locks database file in memory when set to true.
	// It prevents potential page faults, however
	// used memory can't be reclaimed. (UNIX only)
	Mlock bool
}

func (opts *Options) BoltOpts() *bbolt.Options {
	if opts == nil {
		return nil
	}
	return &bbolt.Options{
		Timeout:         opts.Timeout,
		NoGrowSync:      opts.NoGrowSync,
		NoFreelistSync:  opts.NoFreelistSync,
		FreelistType:    opts.FreelistType,
		ReadOnly:        opts.ReadOnly,
		MmapFlags:       opts.MmapFlags,
		InitialMmapSize: opts.InitialMmapSize,
		PageSize:        opts.PageSize,
		NoSync:          opts.NoSync,
		OpenFile:        opts.OpenFile,
		Mlock:           opts.Mlock,
	}
}

var DefaultBBoltOptions = Options{
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

	for _, db := range all.mdbs.dbs {
		if err := db.Close(); err != nil {
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

	var bdb *BBoltDB
	if bdb, err = bbolt.Open(fp, 0o600, opts.BoltOpts()); err != nil {
		return
	}

	db = &DB{
		b: bdb,

		marshalFn:   DefaultMarshalFn,
		unmarshalFn: DefaultUnmarshalFn,
	}

	if opts != nil && opts.InitDB != nil {
		if err = opts.InitDB(db); err != nil {
			return
		}
	}

	if opts != nil && opts.InitialBuckets != nil {
		for _, bucket := range opts.InitialBuckets {
			if err = db.CreateBucket(bucket); err != nil {
				return
			}
		}
	}

	if mdb.m == nil {
		mdb.m = map[string]*DB{}
	}

	mdb.m[name] = db

	db.onClose = func() {
		mdb.mux.Lock()
		delete(mdb.m, name)
		mdb.mux.Unlock()
	}

	return
}

func (mdb *MultiDB) ForEachDB(fn func(name string, db *DB) error) error {
	mdb.mux.RLock()
	dbNames := make([]string, 0, len(mdb.m))
	for name := range mdb.m {
		dbNames = append(dbNames, name)
	}
	mdb.mux.RUnlock()
	for _, name := range dbNames {
		mdb.mux.RLock()
		db := mdb.m[name]
		mdb.mux.RUnlock()
		if db != nil {
			if err := fn(name, db); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mdb *MultiDB) CloseDB(name string) (err error) {
	mdb.mux.Lock()
	defer mdb.mux.Unlock()
	if db := mdb.m[name]; db != nil {
		err = db.b.Close()
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
