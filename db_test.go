package mbbolt

import (
	"log"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func dieIf(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

type S struct {
	X    int
	Y    string
	Blah *S
}

type Test struct {
	fn     string
	bucket string
	key    string
	value  any
}

func TestDB(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp+"/x.db", nil)
	dieIf(t, err)
	defer db.Close()
	defer os.Remove(tmp + "/x.db")

	ch := make(chan bool, 1)
	db.OnSlowUpdate(time.Millisecond*10, func(frs *runtime.Frames, took time.Duration) {
		close(ch)
	})

	tests := []Test{
		{"putget", "b1", "key1", "value"},
		{"putget", "b1", "key2", []byte("value")},
		{"putget", "b1", "key3", &S{42, "answer", nil}},
		{"putget", "b1", "key4", &S{42, "answer", &S{42, "answer", nil}}},
	}

	for _, tst := range tests {
		t.Logf("running: %q", tst)
		switch tst.fn {
		case "putget":
			putGet(t, db, tst)
		}
	}

	db.Update(func(tx *Tx) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	select {
	case <-ch:
		t.Log("slow updated called successfully")
	default:
		t.Fatal("slow update not called")
	}
}

func TestMultiDB(t *testing.T) {
	mdb := NewMultiDB(t.TempDir(), ".db", nil)
	defer mdb.Close()
}

func putGet(tb testing.TB, db *DB, t Test) {
	tb.Helper()
	dieIf(tb, db.Put(t.bucket, t.key, t.value))
	rv := reflect.New(reflect.TypeOf(t.value))
	dieIf(tb, db.Get(t.bucket, t.key, rv.Interface()))
	v := rv.Elem().Interface()
	if !reflect.DeepEqual(v, t.value) {
		tb.Fatalf("expected %#+v, got %#+v", t.value, v)
	}
}

func TestSlow(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp+"/x.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(tmp + "/x.db")

	db.OnSlowUpdate(time.Second, func(frs *runtime.Frames, took time.Duration) {
		buf := FramesToString(frs)
		t.Logf("took %v\n%s", took, buf)
	})
	slowTest(db)
}

func slowTest(db *DB) {
	go slowTest2(db)

	db.Update(func(tx *Tx) error {
		go slowTest2(db)
		time.Sleep(time.Second * 5)
		return nil
	})
	slowTest2(db)
}

func slowTest2(db *DB) {
	db.Update(func(tx *Tx) error {
		time.Sleep(time.Second * 2)
		return nil
	})
}
