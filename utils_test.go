package mbbolt

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"go.oneofone.dev/genh"
)

func TestConvert(t *testing.T) {
	const N = 100000
	tmp := t.TempDir()
	db1, err := Open(context.Background(), filepath.Join(tmp, "1.db"), nil)
	db1.SetMarshaler(genh.MarshalMsgpack, genh.UnmarshalMsgpack)
	db2 := NewSegDB(context.Background(), filepath.Join(tmp, "2"), ".db", nil, 32)

	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	for i := 0; i < N; i++ {
		if err := db1.Put("bucket", fmt.Sprintf("%06d", i), i); err != nil {
			t.Fatal(i, err)
		}
	}

	if err := ConvertDB(db2, db1, func(bucket string, k, v []byte) ([]byte, bool) {
		if string(k) == "000055" {
			v, _ = genh.MarshalMsgpack(9999999999)
		}
		return v, true
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		var v int
		if err := db2.Get("bucket", fmt.Sprintf("%06d", i), &v); err != nil || v != i {
			if v == 9999999999 && i == 55 {
				continue
			}
			t.Fatalf("%v %v %v", i, v, err)
		}
	}
}
