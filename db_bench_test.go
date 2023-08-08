package mbbolt

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkNoFreelistSync(b *testing.B) {
	b.Run("True", func(b *testing.B) {
		tmp := b.TempDir()
		opts := *DefaultOptions
		opts.NoFreelistSync = true
		db := NewSegDB(context.Background(), tmp, ".db", &opts, 32)
		defer db.Close()
		var x atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := x.Add(1)
				db.Put("bkt", strconv.FormatInt(i, 10), i)
			}
		})
	})
	b.Run("False", func(b *testing.B) {
		tmp := b.TempDir()
		opts := *DefaultOptions
		opts.NoFreelistSync = false
		db := NewSegDB(context.Background(), tmp, ".db", &opts, 32)
		defer db.Close()
		var x atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := x.Add(1)
				db.Put("bkt", strconv.FormatInt(i, 10), i)
			}
		})
	})
}
