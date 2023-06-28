package rbolt

import (
	"context"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.oneofone.dev/genh"
	"go.oneofone.dev/gserv"
	"go.oneofone.dev/mbbolt"
	"go.oneofone.dev/oerrs"
)

var (
	RespOK              = gserv.NewMsgpResponse(nil).Cached()
	RespNotFound        = gserv.NewError(http.StatusNotFound, "Not Found")
	RespAlreadyUnlocked = gserv.NewError(http.StatusLocked, "Already Unlocked")

	lg = log.New(log.Default().Writer(), "", log.Lshortfile)
)

const Version = 20230609

func NewServer(dbPath string, dbOpts *mbbolt.Options) *Server {
	srv := &Server{
		s:   gserv.New(gserv.WriteTimeout(time.Minute*10), gserv.ReadTimeout(time.Minute*10), gserv.SetCatchPanics(true)),
		mdb: mbbolt.NewMultiDB(dbPath, ".db", dbOpts),
		j:   newJournal(dbPath, "logs/2006/01/02", true),

		MaxUnusedLock: time.Second * 30,
	}
	return srv.init()
}

func (s *Server) Close() error {
	var el oerrs.ErrorList
	el.PushIf(s.s.Close())
	s.s.Close()
	if s.j != nil {
		el.PushIf(s.j.Close())
	}
	el.PushIf(s.mdb.Close())
	return el.Err()
}

type stats struct {
	ActiveLocks genh.AtomicInt64 `json:"activeLocks"`
	Locks       genh.AtomicInt64 `json:"locks"`
	Timeouts    genh.AtomicInt64 `json:"timeouts"`
	Gets        genh.AtomicInt64 `json:"gets"`
	Puts        genh.AtomicInt64 `json:"puts"`
	Deletes     genh.AtomicInt64 `json:"deletes"`
	Commits     genh.AtomicInt64 `json:"commits"`
	Rollbacks   genh.AtomicInt64 `json:"rollbacks"`
}

type serverTx struct {
	sync.Mutex
	last atomic.Int64
	*mbbolt.Tx
}

type (
	Server struct {
		s   *gserv.Server
		mdb *mbbolt.MultiDB
		j   *journal

		txs   genh.LMap[string, *serverTx]
		stats stats

		MaxUnusedLock time.Duration
		AuthKey       string
	}
)

func (s *Server) init() *Server {
	s.s.Use(func(ctx *gserv.Context) gserv.Response {
		if s.AuthKey != "" && ctx.Req.Header.Get("Authorization") != s.AuthKey {
			ctx.EncodeCodec(gserv.MsgpCodec{}, http.StatusUnauthorized, "Unauthorized")
			return nil
		}
		clearHeaders(ctx)
		return nil
	})

	gserv.MsgpGet(s.s, "/stats", s.getStats, false)
	gserv.JSONGet(s.s, "/stats.json", s.getStats, false)

	gserv.MsgpPost(s.s, "/tx/begin/*db", s.txBegin, false)
	gserv.MsgpDelete(s.s, "/tx/commit/*db", s.txCommit, false)
	gserv.MsgpDelete(s.s, "/tx/rollback/*db", s.txRollback, false)
	gserv.MsgpPost(s.s, "/tx/*db", s.handleTx, false)

	gserv.MsgpPost(s.s, "/noTx/*db", s.handleNoTx, false)

	go s.checkLocks()
	return s
}

func (s *Server) Run(ctx context.Context, addr string) error {
	return s.s.Run(ctx, addr)
}

func (s *Server) getStats(ctx *gserv.Context) (*stats, error) {
	return &s.stats, nil
}

func (s *Server) txBegin(ctx *gserv.Context, req any) ([]byte, error) {
	dbName := ctx.Param("db")
	if dbName == "" {
		dbName = "default"
	}

	db, err := s.mdb.Get(dbName, nil)
	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}

	tts := s.txs.MustGet(dbName, func() *serverTx {
		return &serverTx{}
	})

	tts.Lock()
	defer tts.Unlock()

	if rctx := ctx.Req.Context(); rctx.Err() != nil {
		lg.Printf("%s: txBegin canceled %v", dbName, tts.Tx != nil)
		return nil, tx.Rollback()
	}

	if tts.Tx != nil {
		return nil, gserv.NewError(http.StatusConflict, "tx already exists")
	}

	tts.Tx = tx
	tts.last.Store(time.Now().UnixNano())
	s.stats.Locks.Add(1)
	s.stats.ActiveLocks.Add(1)
	s.j.Write(&journalEntry{Op: "txBegin", DB: dbName}, nil)
	return nil, nil
}

func (s *Server) txCommit(ctx *gserv.Context) ([]byte, error) {
	return s.unlock(ctx.Param("db"), true)
}

func (s *Server) txRollback(ctx *gserv.Context) ([]byte, error) {
	return s.unlock(ctx.Param("db"), false)
}

func (s *Server) unlock(dbName string, commit bool) ([]byte, error) {
	if dbName == "" {
		dbName = "default"
	}

	err := s.withTx(dbName, true, func(tx *mbbolt.Tx) error {
		if commit {
			return tx.Commit()
		}
		return tx.Rollback()
	})

	je := &journalEntry{DB: dbName}
	if commit {
		s.stats.Commits.Add(1)
		je.Op = "txCommit"
	} else {
		s.stats.Rollbacks.Add(1)
		je.Op = "txRollback"
	}
	s.j.Write(je, err)

	s.stats.ActiveLocks.Add(-1)

	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}

	return nil, nil
}

func (s *Server) checkLocks() {
	for {
		for _, dbName := range s.txs.Keys() {
			tx := s.txs.Get(dbName)
			if tx == nil {
				continue
			}

			if time.Duration(time.Now().UnixNano()-tx.last.Load()) > s.MaxUnusedLock {
				lg.Printf("deleted stale lock: %s", dbName)
				s.stats.Timeouts.Add(1)
				if _, err := s.unlock(dbName, false); err != nil {
					lg.Printf("failed to unlock: %s", err)
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (s *Server) withTx(dbName string, rm bool, fn func(tx *mbbolt.Tx) error) error {
	if dbName == "" {
		dbName = "default"
	}
	tx := s.txs.Get(dbName)
	if tx == nil {
		return gserv.ErrNotFound
	}

	tx.last.Store(time.Now().UnixNano())

	tx.Lock()
	defer func() {
		if rm {
			tx.Tx = nil
			tx.last.Store(math.MaxInt64)
		}
		tx.Unlock()
	}()
	if tx.Tx == nil {
		return RespAlreadyUnlocked
	}
	return fn(tx.Tx)
}

func (s *Server) handleTx(ctx *gserv.Context, req *srvReq) (out []byte, err error) {
	dbName := ctx.Param("db")
	if req.Op == opPut {
		if b, ok := req.Value.([]byte); ok {
			out = b
		} else {
			out, _ = genh.MarshalMsgpack(req.Value)
		}
	}
	err = s.withTx(dbName, false, func(tx *mbbolt.Tx) (err error) {
		switch req.Op {
		case opGet:
			if out = tx.GetBytes(req.Bucket, req.Key, true); len(out) == 0 {
				out, err = nil, oerrs.Errorf("key not found: %s::%s", req.Bucket, req.Key)
			}
			return err
		case opPut:
			err = tx.PutBytes(req.Bucket, req.Key, out)
			out = nil
			return
		case opForEach:
			enc := genh.NewMsgpackEncoder(ctx)
			return tx.ForEachBytes(req.Bucket, func(key, val []byte) error {
				err := enc.Encode([2][]byte{key, val})
				ctx.Flush()
				return err
			})
		case opSeq:
			seq, err := tx.NextIndex(req.Bucket)
			if err == nil {
				out, _ = genh.MarshalMsgpack(seq)
			}
			return err
		case opSetSeq:
			var n uint64
			switch v := req.Value.(type) {
			case uint64:
				n = v
			case int64:
				n = uint64(v)
			default:
				return oerrs.Errorf("invalid value type: %T", req.Value)
			}
			err = tx.SetNextIndex(req.Bucket, n)
			return err
		case opDel:
			return tx.Delete(req.Bucket, req.Key)
		default:
			return oerrs.Errorf("unknown op: %s", req.Op)
		}
	})
	je := &journalEntry{Op: "tx" + req.Op.String(), DB: dbName, Bucket: req.Bucket, Key: req.Key, Value: out}
	s.j.Write(je, err)

	if rctx := ctx.Req.Context(); rctx.Err() != nil {
		lg.Printf("%s: tx canceled", dbName)
		return s.unlock(dbName, false)
	}

	if err != nil {
		return nil, gserv.NewError(http.StatusInternalServerError, err)
	}
	return
}

func (s *Server) handleNoTx(ctx *gserv.Context, req *srvReq) (out []byte, err error) {
	dbName := ctx.Param("db")
	if dbName == "" {
		dbName = "default"
	}
	var db *mbbolt.DB
	if db, err = s.mdb.Get(dbName, nil); err != nil {
		return
	}
	switch req.Op {
	case opGet:
		if out, err = db.GetBytes(req.Bucket, req.Key); len(out) == 0 {
			out, err = nil, oerrs.Errorf("key not found: %s::%s", req.Bucket, req.Key)
		}
	case opPut:
		if b, ok := req.Value.([]byte); ok {
			out = b
		} else {
			out, _ = genh.MarshalMsgpack(req.Value)
		}
		err = db.PutBytes(req.Bucket, req.Key, out)
	case opForEach:
		enc := genh.NewMsgpackEncoder(ctx)
		err = db.ForEachBytes(req.Bucket, func(key, val []byte) error {
			err := enc.Encode([2][]byte{key, val})
			ctx.Flush()
			return err
		})
	case opSeq:
		err = db.Update(func(tx *mbbolt.Tx) error {
			seq, err2 := tx.NextIndex(req.Bucket)
			if err2 == nil {
				out, _ = genh.MarshalMsgpack(seq)
			}
			return err
		})
	case opSetSeq:
		err = db.Update(func(tx *mbbolt.Tx) error {
			var n uint64
			switch v := req.Value.(type) {
			case uint64:
				n = v
			case int64:
				n = uint64(v)
			default:
				return oerrs.Errorf("invalid value type: %T", req.Value)
			}
			return tx.SetNextIndex(req.Bucket, n)
		})
	case opDel:
		err = db.Delete(req.Bucket, req.Key)
	default:
		err = oerrs.Errorf("unknown op: %s", req.Op)
	}

	je := &journalEntry{Op: req.Op.String(), DB: dbName, Bucket: req.Bucket, Key: req.Key, Value: out}
	s.j.Write(je, err)
	return
}

func splitPath(p string) (out []string) {
	p = strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
	return strings.Split(p, "/")
}

func clearHeaders(ctx *gserv.Context) {
	h := ctx.Header()
	h["Date"] = nil
	h["Content-Type"] = nil
}
