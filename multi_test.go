package mbbolt

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"go.oneofone.dev/otk"
)

func TestMultiRace(t *testing.T) {
	opts := *DefaultOptions
	opts.AutoRetry = true

	mdb := NewMultiDB(t.TempDir(), ".db", &opts)
	defer mdb.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			mdb.MustGet(context.Background(), "test"+strconv.Itoa(i%3), nil)
		}()
	}
	wg.Wait()
	mdb.Close()
}

func TestMultiBackupRestore(t *testing.T) {
	opts := *DefaultOptions
	opts.AutoRetry = true

	tmp := t.TempDir()
	mdb := NewMultiDB(tmp, ".db", &opts)
	defer mdb.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			mdb.MustGet(context.Background(), "test"+strconv.Itoa(i%3), nil).Put("bucket", "key", i)
		}()
	}
	wg.Wait()
	var buf otk.Buffer // need ReadAt
	if _, err := mdb.Backup(&buf, nil); err != nil {
		t.Fatal(err)
	}
	mdb.Close()
	mdb.m = make(map[string]*DB)
	t.Logf("buf size: %d", buf.Len())
	if err := mdb.Restore(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if len(mdb.m) != 3 {
		t.Fatal("expected 100 dbs, got", len(mdb.m))
	}
	mdb.Close()
}
