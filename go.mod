module go.oneofone.dev/mbbolt

go 1.19

require (
	github.com/vmihailenco/msgpack/v5 v5.3.5
	go.etcd.io/bbolt v1.3.7-0.20221229101948-b654ce922133
	go.oneofone.dev/genh v0.0.0-20230425174533-93aa689f2b5e
	go.oneofone.dev/gserv v1.0.1-0.20230501152402-c82adcf2be2e
)

require (
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.oneofone.dev/oerrs v1.0.7-0.20221003171156-2726106fc553
	go.oneofone.dev/otk v1.0.7
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/image v0.7.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace github.com/vmihailenco/msgpack/v5 v5.3.5 => github.com/alpineiq/msgpack/v5 v5.3.5-no-partial-alloc
