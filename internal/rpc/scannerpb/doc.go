// Package scannerpb holds the generated gRPC/protobuf types for the
// Coordinator<->Worker wire protocol (see scanner.proto), plus
// hand-written conversion helpers (convert.go) to/from the domain types
// in internal/target and internal/output.
//
// Regenerate after editing scanner.proto with `go generate ./...` from
// the repo root, or run the protoc invocation below directly from this
// directory. Requires protoc, protoc-gen-go, and protoc-gen-go-grpc on
// PATH.
//
//	cd internal/rpc/scannerpb && protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative scanner.proto
package scannerpb

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative scanner.proto
