package rbolt

import (
	"bytes"
	"context"
	"io"
	"net/http"
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
	Client struct {
		c     *http.Client
		locks genh.LMap[string, *Tx]
		addr  string

		RetryCount int
		RetrySleep time.Duration
		AuthKey    string
	}
)

func (c *Client) Close() error {
	var el oerrs.ErrorList
	c.locks.ForEach(func(k string, tx *Tx) bool {
		el.PushIf(tx.rollback())
		return true
	})
	return el.Err()
}

func (c *Client) doTx(ctx context.Context, op op, db, bucket, key string, value, out any) (err error) {
	return c.doReq(ctx, "POST", "tx/"+db, &srvReq{Op: op, Bucket: bucket, Key: key, Value: value}, out)
}

func (c *Client) doNoTx(ctx context.Context, op op, db, bucket, key string, value, out any) (err error) {
	return c.doReq(ctx, "POST", "noTx/"+db, &srvReq{Op: op, Bucket: bucket, Key: key, Value: value}, out)
}

func (c *Client) doReq(ctx context.Context, method, url string, body *srvReq, out any) (err error) {
	var resp *http.Response
	var bodyBytes []byte
	if bodyBytes, err = genh.MarshalMsgpack(body); err != nil {
		return
	}

	retry := c.RetryCount
	for {
		req, _ := http.NewRequestWithContext(ctx, method, c.addr+url, bytes.NewReader(bodyBytes))
		if c.AuthKey != "" {
			req.Header.Set("Authorization", c.AuthKey)
		}
		if resp, err = c.c.Do(req); err == nil {
			break
		}
		if ctx.Err() != nil {
			return
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

func (c *Client) NextIndex(ctx context.Context, db, bucket string) (id uint64, err error) {
	err = c.doNoTx(ctx, opSeq, db, bucket, "", nil, &id)
	return
}

func (c *Client) SetNextIndex(ctx context.Context, db, bucket string, id uint64) (err error) {
	err = c.doNoTx(ctx, opSetSeq, db, bucket, "", id, nil)
	return
}

func (c *Client) Get(ctx context.Context, db, bucket, key string, v any) (err error) {
	return c.doNoTx(ctx, opGet, db, bucket, key, nil, v)
}

func (c *Client) Put(ctx context.Context, db, bucket, key string, v any) error {
	if err := c.doNoTx(ctx, opPut, db, bucket, key, v, nil); err != nil {
		return err
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, db, bucket, key string) error {
	if err := c.doNoTx(ctx, opDel, db, bucket, key, nil, nil); err != nil {
		return err
	}
	return nil
}

func (c *Client) Update(ctx context.Context, db string, fn func(tx *Tx) error) error {
	tx, err := c.begin(ctx, db)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if err2 := tx.rollback(); err2 != nil {
			err = oerrs.Errorf("%v: %w", err, err2)
		}
		return err
	}
	return tx.commit()
}

func (c *Client) begin(ctx context.Context, db string) (*Tx, error) {
	if err := c.doReq(ctx, "POST", "tx/begin/"+db, nil, nil); err != nil {
		return nil, err
	}
	tx := &Tx{ctx: ctx, c: c, db: db}
	c.locks.Set(db, tx)
	return tx, nil
}

type Tx struct {
	ctx context.Context
	c   *Client
	db  string
}

func (tx *Tx) NextIndex(bucket string) (id uint64, err error) {
	err = tx.c.doTx(tx.ctx, opSeq, tx.db, bucket, "", nil, &id)
	return
}

func (tx *Tx) SetNextIndex(bucket string, id uint64) (err error) {
	err = tx.c.doTx(tx.ctx, opSetSeq, tx.db, bucket, "", id, nil)
	return
}

func (tx *Tx) Get(bucket, key string, v any) (err error) {
	return tx.c.doTx(tx.ctx, opGet, tx.db, bucket, key, nil, v)
}

func (tx *Tx) Put(bucket, key string, v any) (err error) {
	return tx.c.doTx(tx.ctx, opPut, tx.db, bucket, key, v, nil)
}

func (tx *Tx) Delete(bucket, key string) (err error) {
	return tx.c.doTx(tx.ctx, opDel, tx.db, bucket, key, nil, nil)
}

func (tx *Tx) commit() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.doReq(tx.ctx, "DELETE", "tx/commit/"+tx.db, nil, nil); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) rollback() error {
	gotLock := false
	tx.c.locks.Update(func(m map[string]*Tx) {
		if gotLock = m[tx.db] == tx; gotLock {
			delete(m, tx.db)
		}
	})
	if !gotLock {
		return oerrs.Errorf("no lock for %s", tx.db)
	}
	if err := tx.c.doReq(tx.ctx, "DELETE", "tx/rollback/"+tx.db, nil, nil); err != nil {
		return err
	}
	return nil
}

type decCloser struct {
	*msgpack.Decoder
	io.Closer
}

func Get[T any](ctx context.Context, c *Client, db, bucket, key string) (v T, err error) {
	err = c.Get(ctx, db, bucket, key, &v)
	return
}

func ForEach[T any](ctx context.Context, c *Client, db, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := c.doNoTx(ctx, opForEach, db, bucket, "", nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	return forEach(ctx, dec, bucket, fn)
}

func ForEachTx[T any](tx *Tx, bucket string, fn func(key string, v T) error) error {
	var dec decCloser
	if err := tx.c.doTx(tx.ctx, opForEach, tx.db, bucket, "", nil, &dec); err != nil {
		return err
	}
	defer dec.Close()
	return forEach(tx.ctx, dec, bucket, fn)
}

func forEach[T any](ctx context.Context, dec decCloser, bucket string, fn func(key string, v T) error) error {
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
		if err := fn(key, v); err != nil {
			return err
		}

	}
}
