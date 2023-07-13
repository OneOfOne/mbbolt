package mbbolt

import (
	"context"
	"errors"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
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

type dbTest struct {
	fn     string
	bucket string
	key    string
	value  any
}

func TestDB(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(context.Background(), tmp+"/x.db", nil)
	dieIf(t, err)
	defer db.Close()
	defer os.Remove(tmp + "/x.db")

	ch := make(chan bool, 1)
	db.OnSlowUpdate(time.Millisecond*10, func(frs *runtime.Frames, took time.Duration) {
		close(ch)
	})

	tests := []dbTest{
		{"putget", "b1", "key1", "value"},
		{"putget", "b1", "key2", []byte("value")},
		{"putget", "b1", "key3", &S{42, "answer", nil}},
		{"putget", "b1", "key4", &S{42, "answer", &S{24, "rewsna", nil}}},
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

func putGet(tb testing.TB, db *DB, t dbTest) {
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
	db, err := Open(context.Background(), tmp+"/x.db", nil)
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

func TestCachedBucket(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(context.Background(), tmp+"/x.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer os.Remove(tmp + "/x.db")
	N := 101
	db.Update(func(tx *Tx) error {
		tx.MustBucket("ints")
		for i := 0; i < N; i++ {
			tx.PutValue("ints", strconv.Itoa(i), i)
		}
		return nil
	})

	cb := CacheOf[int](db, "ints", true)

	for i := 0; i < 10; i++ {
		i := i + N
		if err := cb.Put(strconv.Itoa(i), i); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N+10; i++ {
		if v, _ := cb.Get(strconv.Itoa(i)); v != i {
			t.Fatalf("%d != %d", i, v)
		}
	}

	for i := 0; i < 10; i++ {
		i := i + N + 10
		if v, _ := cb.Get(strconv.Itoa(i)); v != 0 {
			t.Fatalf("v != 0")
		}
	}
}

func TestCanceledCtx(t *testing.T) {
	ctx, cfn := context.WithCancel(context.Background())
	tmp := t.TempDir()
	db, err := Open(ctx, tmp+"/x.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfn() // cancel the context

	if err := db.Put("x", "x", "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected err: %v", err)
	}
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
