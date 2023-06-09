package rbolt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type S struct {
	S *S
	A string
	B int64
	C float64
}

func TestClient(t *testing.T) {
	const dbName = "shinyDB"
	const bucketName = "someBucket"

	rbs := NewServer(t.TempDir(), nil)
	rbs.AuthKey = "da3b361b0a16be5c31e5ef87eb4a48dcd3c1d0c9"
	defer rbs.Close()
	rbs.MaxUnusedLock = time.Second / 10
	// defer rbs.Close()
	go rbs.Run(context.Background(), ":0")

	time.Sleep(time.Millisecond * 100)
	url := "http://" + rbs.s.Addrs()[0]
	t.Log("srv addr", url)

	t.Run("NoTx", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		defer c.Close()
		sp := &S{A: "test", B: 123, C: 123.456, S: &S{A: "-", B: 321, C: 654.321}}
		if err := c.Put(dbName, bucketName, "key", sp); err != nil {
			t.Fatal(err)
		}

		var s S
		if err := c.Get(dbName, bucketName, "key", &s); err != nil {
			t.Fatal(err)
		}

		if s.A != sp.A || s.B != sp.B || s.C != sp.C || s.S.A != sp.S.A || s.S.B != sp.S.B || s.S.C != sp.S.C {
			t.Fatal("invalid data", s, sp)
		}

		found := false
		if err := ForEach(c, dbName, bucketName, func(key string, ss *S) error {
			if key == "key" && ss.A == "test" && ss.B == 123 && ss.C == 123.456 {
				found = true
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatal("foreach failed")
		}

		if err := c.Delete(dbName, bucketName, "key"); err != nil {
			t.Fatal(err)
		}

		if err := c.Get(dbName, bucketName, "key", &s); err == nil {
			t.Fatal("expected error", s)
		}
	})

	t.Run("Tx", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		defer c.Close()
		if err := c.Update(dbName, func(tx *Tx) error {
			tx.SetNextIndex(bucketName, 100)
			for i := 0; i < 100; i++ {
				id, err := tx.NextIndex(bucketName)
				if err != nil {
					t.Error(err)
					return err
				}
				if err := tx.Put(bucketName, strconv.Itoa(int(id)+1000), &S{A: "test", S: &S{B: int64(id)}}); err != nil {
					t.Error(err)
					return err
				}
			}

			found := false
			if err := ForEachTx(tx, bucketName, func(key string, ss *S) error {
				if key == "1105" {
					if ss.S == nil || ss.S.B != 105 {
						return fmt.Errorf("unexpected value: %+v %+v", ss, ss.S)
					}
					found = true
				}
				return nil
			}); err != nil {
				t.Error(err)
				return err
			}

			if !found {
				return errors.New("foreach failed")
			}

			var s S
			if err := tx.Get(bucketName, "1105", &s); err != nil || s.A != "test" || s.S.B != 105 {
				return fmt.Errorf("unexpected error: %w %+v %+v", err, s, s.S)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		var s S
		if err := c.Get(dbName, bucketName, "1105", &s); err != nil {
			t.Fatal(err)
		}
		if s.S == nil || s.S.B != 105 {
			t.Fatal("expected s.S.B == 105")
		}

		if err := c.Update(dbName, func(tx *Tx) error {
			if err := tx.Delete(bucketName, "1105"); err != nil {
				return err
			}
			var s S
			if err := tx.Get(bucketName, "1105", &s); err == nil {
				t.Fatal("expected error", s)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := c.Get(dbName, bucketName, "1105", &s); err == nil {
			t.Fatal(err)
		}
	})

	t.Run("AutoUnlock", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		defer c.Close()
		err := c.Update(dbName, func(tx *Tx) error {
			if err := tx.Put(bucketName, "1005", &S{A: "test", S: &S{B: 5}}); err != nil {
				return err
			}
			time.Sleep(time.Second * 2)
			if err := tx.Put(bucketName, "1005", &S{A: "test", S: &S{B: 5}}); err == nil {
				t.Error("expected error")
			}
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("BugDecodingSimpleTypes", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		defer c.Close()
		if err := c.Put(dbName, bucketName+"2", "string", "str"); err != nil {
			t.Fatal(err)
		}
		var str string
		if err := c.Get(dbName, bucketName+"2", "string", &str); err != nil || str != "str" {
			t.Fatal("unexpected error", err, str)
		}

		if err := c.Get(dbName, bucketName+"2", "string", &str); err != nil || str != "str" {
			t.Fatal("unexpected error", err, str)
		}
	})

	t.Run("BugBadNames", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		defer c.Close()
		if err := c.Put(dbName, bucketName+"2", ".", "str"); err != nil {
			t.Fatal(err)
		}
		var str string

		if err := c.Get(dbName, bucketName+"2", ".", &str); err != nil || str != "str" {
			t.Fatal("unexpected error", err, str)
		}
		if err := c.Put(dbName, bucketName+"2", "#", "str"); err != nil {
			t.Fatal(err)
		}

		if err := c.Get(dbName, bucketName+"2", "#", &str); err != nil || str != "str" {
			t.Fatal("unexpected error", err, str)
		}
	})

	t.Run("Auth", func(t *testing.T) {
		c := NewClient(url, rbs.AuthKey)
		// c.AuthKey = rbs.AuthKey
		defer c.Close()
		c.AuthKey = ""
		if err := c.Put("", bucketName, "11111", &S{A: "test", S: &S{B: 5}}); err == nil {
			t.Fatal("expected error")
		}
		c.AuthKey = rbs.AuthKey

		if err := c.Put("", bucketName, "11111", &S{A: "test", S: &S{B: 5}}); err != nil {
			t.Fatal("unexpected error")
		}

		var s S
		if err := c.Get("", bucketName, "11111", &s); err != nil {
			t.Fatal("unexpected error", err, s)
		}
		if s.S == nil || s.S.B != 5 {
			t.Fatal("expected s.S.B == 5", s.S)
		}
	})

	t.Run("CheckLog", func(t *testing.T) {
		f := rbs.j.f
		f.Sync()
		fn := f.Name()
		f.Seek(0, 0)
		dec := json.NewDecoder(f)
		t.Log(fn)
		cnt := 0
		for {
			var je journalEntry
			if err := dec.Decode(&je); err != nil {
				if !errors.Is(err, io.EOF) {
					t.Error(err)
				}
				break
			}
			cnt++
			// t.Log(je)
		}
		// update this when the test changes
		if cnt != 229 {
			t.Error("unexpected number of journal entries", cnt)
		}
		t.Logf("total %d entries", cnt)
	})
}
