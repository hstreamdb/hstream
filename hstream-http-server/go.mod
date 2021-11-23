module github.com/hstreamdb/hstream/hstream-http-server

go 1.14

replace github.com/hstreamdb/hstream/hstream-http-server => ./
replace github.com/hstreamdb/hstream/common => ../common

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.6.0
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.42.0
	google.golang.org/protobuf v1.27.1
)
