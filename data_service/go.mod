module github.com/synthos/data-service

go 1.24.0

toolchain go1.24.5

replace github.com/tafolabi009/backend/proto => ../proto/gen/go

require (
	github.com/lib/pq v1.10.9
	github.com/tafolabi009/backend/proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.76.0
)

require (
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251111163417-95abcf5c77ba // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
