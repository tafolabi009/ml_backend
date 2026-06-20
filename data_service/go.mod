module github.com/synthos/data-service

go 1.24.0

toolchain go1.24.5

replace github.com/tafolabi009/backend/proto => ../proto/gen/go

require (
	github.com/jackc/pgx/v5 v5.7.6
	github.com/tafolabi009/backend/proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.76.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.44.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251111163417-95abcf5c77ba // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
