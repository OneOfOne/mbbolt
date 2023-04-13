package mbbolt

import (
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestSegDB(t *testing.T) {
	t.Run("OpenCloseLeak", func(t *testing.T) {
		d := t.TempDir()
		seg := NewSegDB(d, ".db", nil, 32)
		seg.Close()
		seg = NewSegDB(d, ".db", nil, 32)
		defer seg.Close()
	})
	t.Run("SegmentFn", func(t *testing.T) {
		m := [10]int{}
		for i := 0; i < 1000; i++ {
			m[DefaultSegmentByKey(strconv.Itoa(i))%10]++
		}
		for i, v := range &m {
			if v < 50 {
				t.Errorf("segment %d has %d values", i, v)
			}
		}
	})
}

func TestSegBackupRestore(t *testing.T) {
	tmp := t.TempDir()
	sdb := NewSegDB(tmp, ".db", nil, 32)
	defer sdb.Close()
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			sdb.Put("bucket", "key"+strconv.Itoa(i), i)
		}()
	}
	wg.Wait()
	zfp := filepath.Join(tmp, "segdb.zip")

	if _, err := sdb.mdb.BackupToFile(zfp, nil); err != nil {
		t.Fatal(err)
	}
	if err := sdb.Close(); err != nil {
		t.Fatal(err)
	}
	sdb, err := NewSegDBFromFile(zfp, tmp, ".db", nil)
	if err != nil {
		t.Fatal(err)
	}

	if ln := len(sdb.dbs); ln != 32 {
		t.Fatal("expected 100 dbs, got", ln)
	}

	for i := 0; i < 10000; i++ {
		var v int
		sdb.Get("bucket", "key"+strconv.Itoa(i), &v)
		if v != i {
			t.Fatalf("expected %d, got %d", i, v)
		}
	}
}
