package mbbolt

import (
	"sync"
	"testing"
)

func TestMultiRace(t *testing.T) {
	mdb := NewMultiDB(t.TempDir(), ".db", nil)
	defer mdb.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mdb.MustGet("test", nil)
		}()
	}
	wg.Wait()
}
