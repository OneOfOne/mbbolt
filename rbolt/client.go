package rbolt

import (
	"bytes"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.oneofone.dev/genh"
	"go.oneofone.dev/gserv"
	"go.oneofone.dev/oerrs"
	"go.oneofone.dev/otk"
)

func NewClient(addr, auth string) *Client {
	if !strings.HasSuffix(addr, "/") {
		addr += "/"
	}
	return &Client{
		c:    gserv.H2Client(),
		addr: addr,

		RetryCount: 100,
		RetrySleep: time.Millisecond * 100,
		AuthKey:    auth,
	}
}

type (
	bucketKeyVal = genh.LMultiMap[string, string, any]
	Client       struct {
		c     *http.Client
		locks genh.LMap[string, *Tx]
		m     genh.LMap[string, *bucketKeyVal]
		addr  string

		RetryCount int
		RetrySleep time.Duration
		AuthKey    string
	}
)

func (c *Client) Close() error {
	var el oerrs.ErrorList
	c.locks.ForEach(func(k string, tx *Tx) bool {
		el.PushIf(tx.Rollback())
		return true
	})
	return el.Err()
}

func (c *Client) ClearCache() {
	c.m.Clear()
}

func (c *Client) doTx(op op, db, bucket, key string, value, out any) (err error) {
	return c.doReq("POST", "tx/"+db, &srvReq{Op: op, Bucket: bucket, Key: key, Value: value}, out)
}

func (c *Client) doNoTx(op op, db, bucket, key string, value, out any) (err error) {
	return c.doReq("POST", "noTx/"+db, &srvReq{Op: op, Bucket: bucket, Key: key, Value: value}, out)
}

func (c *Client) doReq(method, url string, body *srvReq, out any) (err error) {
	var resp *http.Response
	var bodyBytes []byte
	if bodyBytes, err = genh.MarshalMsgpack(body); err != nil {
		return
	}

	retry := c.RetryCount
	for {
		req, _ := http.NewRequest(method, c.addr+url, bytes.NewReader(bodyBytes))
		if c.AuthKey != "" {
			req.Header.Set("Authorization", c.AuthKey)
		}
		if resp, err = c.c.Do(req); err == nil {
			break
		}
		if retry--; retry < 1 {
			return oerrs.ErrorCallerf(2, "failed after %d retires: %w", c.RetryCount, err)
		}
		time.Sleep(c.RetrySleep)
	}

	// log.Println(method, url, string(body))
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized {
			return oerrs.Errorf("unauthorized")
		}
		var r gserv.Error
		if err := genh.DecodeMsgpack(resp.Body, &r); err != nil {
			return oerrs.Errorf("error decoding response for %s %s (%v): %v", method, url, resp.StatusCode, err)
		}
		return r
	}

	if out, ok := out.(*decCloser); ok {
		*out = decCloser{genh.NewMsgpackDecoder(resp.Body), resp.Body}
		return nil
	}

	defer resp.Body.Close()
	if out == nil {
		return nil
	}

	return genh.DecodeMsgpack(resp.Body, out)
}

func (c *Client) cache(db string) *bucketKeyVal {
	return c.m.MustGet(db, func() *bucketKeyVal {
		return &bucketKeyVal{}
	})
}

func (c *Client) NextIndex(db, bucket string) (id uint64, err error) {
	err = c.doNoTx(opSeq, db, bucket, "", nil, &id)
	return
}

func (c *Client) SetNextIndex(db, bucket string, id uint64) (err error) {
	err = c.doNoTx(opSetSeq, db, bucket, "", id, nil)
	return
}

func (c *Client) Get(db, bucket, key string, v any) (err error) {
	vv := c.cache(db).MustGet(bucket, key, func() any {
		err = c.doNoTx(opGet, db, bucket, key, nil, v)
		return reflect.ValueOf(v).Elem().Interface()
	})
	if err != nil {
		c.cache(db).DeleteChild(bucket, key)
		return
	}
	genh.ReflectClone(reflect.ValueOf(v).Elem(), reflect.Indirect(reflect.ValueOf(vv)), true)
	return
}

func (c *Client) Put(db, bucket, key string, v any) error {
	if err := c.doNoTx(opPut, db, bucket, key, v, nil); err != nil {
		return err
	}
	c.cache(db).Set(bucket, key, v)
	return nil
}

func (c *Client) Delete(db, bucket, key string) error {
	if err := c.doNoTx(opDel, db, bucket, key, nil, nil); err != nil {
		return err
	}
	c.cache(db).DeleteChild(bucket, key)
	return nil
}

func (c *Client) Update(db string, fn func(tx *Tx) error) error {
	tx, err := c.Begin(db)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if err2 := tx.Rollback(); err2 != nil {
			err = oerrs.Errorf("%v: %w", err, err2)
		}
		return err
	}
	return tx.Commit()
}

func (c *Client) Begin(db string) (*Tx, error) {
	if err := c.doReq("POST", "tx/begin/"+db, nil, nil); err != nil {
		return nil, err
	}
	tx := &Tx{c: c, db: db, prefix: "tx/" + db + "/"}
	c.locks.Set(db, tx)
	return tx, nil
}

type Tx struct {
	c      *Client
	db     string
	prefix string

	updates []func()
}

func (tx *Tx) NextIndex(bucket string) (id uint64, err error) {
	err = tx.c.doTx(opSeq, tx.db, bucket, "", nil, &id)
	return
}

func (tx *Tx) SetNextIndex(bucket string, id uint64) (err error) {
	err = tx.c.doTx(opSetSeq, tx.db, bucket, "", id, nil)
	return
}

func (tx *Tx) Get(bucket, key string, v any) (err error) {
	return tx.c.doTx(opGet, tx.db, bucket, key, nil, v)
}

func (tx *Tx) Put(bucket, key string, v any) (err error) {
	if err = tx.c.doTx(opPut, tx.db, bucket, key, v, nil); err == nil {
		tx.updates = append(tx.updates, func() {
			tx.c.cache(tx.db).Set(bucket, key, v)
		})
	}
	return
}

func (tx *Tx) Delete(bucket, key string) (err error) {
	if err = tx.c.doTx(opDel, tx.db, bucket, key, nil, nil); err == nil {
		tx.updates = append(tx.updates, func() {
			tx.c.cache(tx.db).DeleteChild(bucket, key)
		})
	}
	return
}

func (tx *Tx) Commit() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.doReq("DELETE", "tx/commit/"+tx.db, nil, nil); err != nil {
		return err
	}
	for _, fn := range tx.updates {
		fn()
	}
	return nil
}

func (tx *Tx) Rollback() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.doReq("DELETE", "tx/rollback/"+tx.db, nil, nil); err != nil {
		return err
	}
	return nil
}

type decCloser struct {
	*msgpack.Decoder
	io.Closer
}

func Get[T any](c *Client, db, bucket, key string) (v T, err error) {
	err = c.Get(db, bucket, key, &v)
	return
}

func ForEach[T any](c *Client, db, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := c.doNoTx(opForEach, db, bucket, "", nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	return forEach(dec, c.cache(db), bucket, fn)
}

func ForEachTx[T any](tx *Tx, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := tx.c.doTx(opForEach, tx.db, bucket, "", nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	return forEach(dec, tx.c.cache(tx.db), bucket, fn)
}

func forEach[T any](dec decCloser, cache *bucketKeyVal, bucket string, fn func(key string, v T) error) error {
	for {
		var kv [2][]byte
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(kv[0]) == 0 {
			continue
		}
		var v T
		if err := genh.UnmarshalMsgpack(kv[1], &v); err != nil {
			return err
		}
		key := otk.UnsafeString(kv[0])
		cache.Set(bucket, key, v)
		if err := fn(key, v); err != nil {
			return err
		}

	}
}
