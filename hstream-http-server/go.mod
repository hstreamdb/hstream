module github.com/hstreamdb/hstream/hstream-http-server

go 1.14

replace github.com/hstreamdb/hstream/hstream-http-server => ./

replace github.com/hstreamdb/hstream/common/api => ../common/api

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.6.0
	github.com/hstreamdb/hstream/common/api v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.42.0
	google.golang.org/protobuf v1.27.1
)
