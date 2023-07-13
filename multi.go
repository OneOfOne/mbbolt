package mbbolt

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"go.oneofone.dev/oerrs"
	"go.oneofone.dev/otk"
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

var DefaultOptions = &Options{
	Timeout:        time.Second, // don't block indefinitely if the db isn't closed
	NoFreelistSync: true,        // improves write performance, slow load if the db isn't closed cleanly
	NoGrowSync:     false,
	FreelistType:   bbolt.FreelistMapType,

	MaxBatchSize:  512,
	MaxBatchDelay: time.Millisecond * 10,

	// syscall.MAP_POPULATE on linux 2.6.23+ does sequential read-ahead
	// which can speed up entire-database read with boltdb.
	MmapFlags: DefaultMMapFlags,

	InitialMmapSize: 1 << 29, // 512MiB
}

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

	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	MaxBatchDelay time.Duration

	MarshalFn   MarshalFn
	UnmarshalFn UnmarshalFn
}

func (opts *Options) Clone() *Options {
	if opts == nil {
		opts = DefaultOptions
	}
	cp := *opts
	return &cp
}

func (opts *Options) BoltOpts() *bbolt.Options {
	if opts == nil {
		opts = DefaultOptions
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

var all struct {
	mdbs struct {
		dbs []*MultiDB
		sync.Mutex
	}
	MultiDB
}

func Open(ctx context.Context, path string, opts *Options) (*DB, error) {
	return all.Get(ctx, path, opts)
}

func MustOpen(ctx context.Context, path string, opts *Options) *DB {
	return all.MustGet(ctx, path, opts)
}

func CloseAll() error {
	var el oerrs.ErrorList
	el.PushIf(all.Close())
	all.mdbs.Lock()
	defer all.mdbs.Unlock()

	for _, db := range all.mdbs.dbs {
		el.PushIf(db.Close())
	}

	return el.Err()
}

func NewMultiDB(prefix, ext string, opts *Options) *MultiDB {
	if opts == nil {
		opts = DefaultOptions
	}

	mdb := &MultiDB{opts: opts, prefix: prefix, ext: ext}
	all.mdbs.Lock()
	all.mdbs.dbs = append(all.mdbs.dbs, mdb)
	all.mdbs.Unlock()
	return mdb
}

type MultiDB struct {
	m      map[string]*DB
	opts   *Options
	prefix string
	ext    string
	mux    sync.RWMutex
}

func (mdb *MultiDB) MustGet(ctx context.Context, name string, opts *Options) *DB {
	db, err := mdb.Get(ctx, name, opts)
	if err != nil {
		log.Panicf("MustGet (%s): %v", name, err)
	}
	return db
}

func (mdb *MultiDB) Get(ctx context.Context, name string, opts *Options) (db *DB, err error) {
	fp := mdb.getPath(name)
	os.MkdirAll(filepath.Dir(fp), 0o755)

	if err = ctx.Err(); err != nil {
		return
	}

	mdb.mux.RLock()
	if db = mdb.m[name]; db != nil {
		mdb.mux.RUnlock()
		return
	}
	mdb.mux.RUnlock()

	if opts == nil {
		opts = mdb.opts
	}

	if opts == nil {
		opts = DefaultOptions
	}

	var bdb *BBoltDB

	for {
		err = nil
		mdb.mux.RLock()
		db = mdb.m[name]
		mdb.mux.RUnlock()

		if db != nil { // concurrency check
			return
		}

		if err = ctx.Err(); err != nil {
			return
		}

		if bdb, err = bbolt.Open(fp, 0o600, opts.BoltOpts()); err != nil && err != bbolt.ErrTimeout {
			return
		}

		if err == nil {
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	mdb.mux.Lock()
	defer mdb.mux.Unlock()

	return mdb.get(ctx, bdb, name, opts)
}

func (mdb *MultiDB) get(ctx context.Context, bdb *BBoltDB, name string, opts *Options) (db *DB, err error) {
	// race check
	if db = mdb.m[name]; db != nil {
		return
	}

	if opts.MaxBatchDelay > 0 {
		bdb.MaxBatchDelay = opts.MaxBatchDelay
	}

	if opts.MaxBatchSize > 0 {
		bdb.MaxBatchSize = opts.MaxBatchSize
	}

	db = &DB{
		ctx: ctx,
		b:   bdb,

		marshalFn:   DefaultMarshalFn,
		unmarshalFn: DefaultUnmarshalFn,
	}

	if opts.MarshalFn != nil {
		db.marshalFn = opts.MarshalFn
	}

	if opts.UnmarshalFn != nil {
		db.unmarshalFn = opts.UnmarshalFn
	}

	if opts.InitDB != nil {
		if err = opts.InitDB(db); err != nil {
			return
		}
	}

	if opts.InitialBuckets != nil {
		if err = db.Update(func(tx *Tx) error {
			for _, bucket := range opts.InitialBuckets {
				if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return
		}
	}

	if mdb.m == nil {
		mdb.m = map[string]*DB{}
	}

	db.onClose = func() {
		mdb.mux.Lock()
		delete(mdb.m, name)
		mdb.mux.Unlock()
	}

	mdb.m[name] = db
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

func (mdb *MultiDB) BackupToDir(dir string, filter func(name string, db *DB) bool) (n int64, err error) {
	mdb.mux.RLock()
	dbNames := make([]string, 0, len(mdb.m))
	for name, db := range mdb.m {
		if filter == nil || filter(name, db) {
			dbNames = append(dbNames, name)
		}
	}
	mdb.mux.RUnlock()

	for _, name := range dbNames {
		mdb.mux.RLock()
		db := mdb.m[name]
		mdb.mux.RUnlock()
		if db == nil {
			continue
		}

		fp := filepath.Join(dir, name+mdb.ext)
		os.MkdirAll(filepath.Dir(fp), 0o755)

		var n2 int64
		if n2, err = db.BackupToFile(fp); err != nil {
			err = oerrs.Errorf("backup %s: %v", fp, err)
			return
		}
		n += n2
	}
	return 0, nil
}

func (mdb *MultiDB) BackupToFile(fp string, filter func(name string, db *DB) bool) (n int64, err error) {
	var f *os.File
	if f, err = os.Create(fp); err != nil {
		return
	}
	defer func() {
		if err2 := f.Close(); err2 != nil {
			if err != nil {
				err2 = fmt.Errorf("multiple errors: %v, %v", err, err2)
			}
			err = err2
		}
	}()
	return mdb.Backup(f, filter)
}

func (mdb *MultiDB) Backup(w io.Writer, filter func(name string, db *DB) bool) (n int64, err error) {
	mdb.mux.RLock()
	dbNames := make([]string, 0, len(mdb.m))
	for name, db := range mdb.m {
		if filter == nil || filter(name, db) {
			dbNames = append(dbNames, name)
		}
	}
	mdb.mux.RUnlock()

	buf := getBuf(w)
	defer putBufAndFlush(buf)

	z := zip.NewWriter(buf)
	defer z.Close()

	for _, name := range dbNames {
		mdb.mux.RLock()
		db := mdb.m[name]
		mdb.mux.RUnlock()
		if db == nil {
			continue
		}

		fp := name + mdb.ext
		w, err2 := z.Create(fp)
		if err2 != nil {
			err = oerrs.Errorf("zip %s: %w", fp, err2)
			return
		}
		var n2 int64
		if n2, err = db.Backup(w); err != nil {
			err = oerrs.Errorf("backup %s: %w", fp, err)
			return
		}
		n += n2
	}
	return 0, nil
}

func (mdb *MultiDB) RestoreFromFile(ctx context.Context, fp string) (err error) {
	var f *os.File
	if f, err = os.Open(fp); err != nil {
		return
	}
	defer f.Close()
	return mdb.Restore(ctx, f)
}

func (mdb *MultiDB) Restore(ctx context.Context, r io.ReaderAt) (err error) {
	mdb.mux.Lock()
	defer mdb.mux.Unlock()
	var names []string
	if err = otk.Unzip(r, mdb.prefix, func(fp string, f *zip.File) bool {
		name := strings.TrimPrefix(strings.TrimSuffix(fp, mdb.ext), mdb.prefix)
		name = strings.TrimPrefix(name, "/")
		names = append(names, name)
		return true
	}); err != nil {
		return
	}

	for _, name := range names {
		// log.Println("restoring", name)
		if err = mdb.restoreOne(ctx, name, mdb.opts); err != nil {
			return
		}
	}

	return
}

func (mdb *MultiDB) restoreOne(ctx context.Context, name string, opts *Options) (err error) {
	if odb := mdb.m[name]; odb != nil {
		odb.onClose = nil
		if err = odb.Close(); err != nil {
			return
		}
		delete(mdb.m, name)
	}
	var bdb *BBoltDB
	if bdb, err = bbolt.Open(mdb.getPath(name), 0o600, opts.BoltOpts()); err != nil {
		return
	}
	_, err = mdb.get(ctx, bdb, name, opts)
	return
}

func (mdb *MultiDB) Close() error {
	mdb.mux.Lock()
	defer mdb.mux.Unlock()
	el := oerrs.NewSafeList(true)

	var wg sync.WaitGroup
	for k, db := range mdb.m {
		k, db := k, db
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.onClose = nil // we're handling this
			if err := db.Close(); err != nil {
				el.Errorf("%s: %v", k, err)
			}
		}()

	}
	wg.Wait()
	mdb.m = nil
	return el.Err()
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
