package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"go.oneofone.dev/mbbolt/rbolt"
	"go.oneofone.dev/oerrs"
)

var (
	port       int
	clientMode bool
	saddr      string
	dbPath     string
	authKey    string
)

func init() {
	log.SetFlags(log.Lshortfile)
	flag.IntVar(&port, "port", 8099, "port to listen on")
	flag.StringVar(&dbPath, "path", "./dbs", "path to store dbs")
	flag.StringVar(&saddr, "srv", "http://127.0.0.1:8099", "path to server")
	flag.StringVar(&authKey, "auth", "auth", "authKey")
	flag.BoolVar(&clientMode, "c", false, "client mode")
	flag.Parse()
}

const ErrUsage = oerrs.String("invalid args, must be [get|put|delete] db bucket [key|NEW] [value|-]")

func main() {
	if !clientMode {
		serve()
		return
	}
	args := flag.Args()
	if len(args) < 4 {
		log.Fatal(ErrUsage)
	}
	cli := rbolt.NewClient(saddr, authKey)
	defer cli.Close()

	err := cli.Update(context.Background(), args[1], func(tx *rbolt.Tx) error {
		switch args[0] {
		case "put":
			if len(args) < 5 {
				log.Fatal(ErrUsage)
			}
			var key string
			if args[3] == "NEW" {
				n, err := tx.NextIndex(args[2])
				if err != nil {
					return err
				}
				key = strconv.FormatUint(n+1000, 10)
			} else {
				key = args[3]
			}
			if args[4] == "-" {
				b, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				args[4] = string(b)
			}
			if err := tx.Put(args[2], key, args[4]); err != nil {
				return err
			}
			log.Printf("PUT %s %s %s", args[1], args[2], key)
			return nil

		case "get":
			if len(args) < 4 {
				return ErrUsage
			}
			var v string
			if err := tx.Get(args[2], args[3], &v); err != nil {
				return err
			}
			log.Printf("GET %s %s %s: %s", args[1], args[2], args[3], v)
			return nil
		case "delete":
			if len(args) < 4 {
				return ErrUsage
			}
			if err := tx.Delete(args[2], args[3]); err != nil {
				return err
			}
			log.Printf("DELETE %v %v %v", args[1], args[2], args[3])
		default:
			log.Fatal("invalid args, must be [get|put|delete] db bucket [key|NEW] [value]")
		}
		return nil
	})
	if err != nil {
		log.Println("failed:", err)
	}
}

func serve() {
	ctx, cfn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cfn()
	os.MkdirAll(dbPath, 0o755)
	srv := rbolt.NewServer(dbPath, nil)
	srv.AuthKey = authKey
	defer srv.Close()
	go func() {
		defer cfn()
		if err := srv.Run(ctx, ":"+strconv.Itoa(port)); err != nil {
			log.Panic(err)
		}
	}()
	log.Printf("[rbolt] Listening on 0.0.0.0:%v", port)
	<-ctx.Done()
}
