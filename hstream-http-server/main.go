package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	hstreamApi "github.com/hstreamdb/hstream-http-server/hstream-api/build/proto/HStream/Server"
	hstream_http_server "github.com/hstreamdb/hstream-http-server/hstream-api/src"
	"google.golang.org/grpc"
)

var (
	gRPCServerHost = flag.String("gRPCServerHost", "127.0.0.1", "gRPCServerHost")
	gRPCServerPort = flag.String("gRPCServerPort", "6570", "gRPCServerPort")
	httpServerPort = flag.String("httpServerPort", "6580", "httpServerPort")
	hpCtx          hstream_http_server.HostPortCtx
)

func main() {
	flag.Parse()
	log.Println("Server is configured with:")
	log.Printf("    gRPCServerHost: %v\n", *gRPCServerHost)
	log.Printf("    gRPCServerPort: %v\n", *gRPCServerPort)
	log.Printf("    httpServerPort: %v\n", *httpServerPort)
	hpCtx = hstream_http_server.HostPortCtx{
		GRPCServerHost: gRPCServerHost,
		GRPCServerPort: gRPCServerPort,
		HttpServerPort: httpServerPort,
	}

	log.Println("Setting gRPC connection")
	conn, err := grpc.DialContext(context.Background(),
		*gRPCServerHost+":"+*gRPCServerPort,
		grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	log.Println("Setting HTTP server")
	gwMux := runtime.NewServeMux()
	if err := hstreamApi.RegisterHStreamApiHandler(context.Background(),
		gwMux,
		conn); err != nil {
		log.Fatalln(err)
	}

	// Custom route for appending base64-encoded payloads
	err = handleAppend(gwMux)
	if err != nil {
		log.Fatalln(err)
	}

	gwServer := &http.Server{Addr: ":" + *httpServerPort, Handler: gwMux}
	log.Printf("Server started on port %v\n", *httpServerPort)
	log.Fatalln(gwServer.ListenAndServe())

}

// ---------------------------------------------------------------------------------------------------------------------

func handleAppend(s *runtime.ServeMux) error {
	return s.HandlePath("POST", "/streams/{streamName}:publish", hstream_http_server.DecodeHandler(s, &hpCtx))
}
