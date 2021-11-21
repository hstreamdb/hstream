package hstream_http_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"

	hstreamApi "github.com/hstreamdb/hstream-http-server/hstream-api/build/proto/HStream/Server"

	"log"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"google.golang.org/grpc"
)

type B64Payload struct {
	Payload string `json:"payload"`
}

func DecodeHandler(mux *runtime.ServeMux, hpCtx *HostPortCtx) func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
		decodeHandlerWith(mux, hpCtx, w, r, pathParams)
	}
}

func decodeHandlerWith(mux *runtime.ServeMux, hpCtx *HostPortCtx, w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
	var (
		in B64Payload
	)
	_, outboundMarshaler := runtime.MarshalerForRequest(mux, r)

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		log.Fatalln(err)
	}
	out, err := base64.StdEncoding.DecodeString(in.Payload)
	if err != nil {
		log.Fatalln(err) // #TODO: 400
	}
	records := bytes.Split(out, []byte("\n"))

	conn, err := grpc.Dial(*hpCtx.GRPCServerHost+":"+*hpCtx.GRPCServerPort,
		grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	defer func(conn *grpc.ClientConn) {
		if err := conn.Close(); err != nil {
			log.Fatalln(err)
		}
	}(conn)
	c := hstreamApi.NewHStreamApiClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := c.Append(ctx, &hstreamApi.AppendRequest{
		StreamName: pathParams["streamName"],
		Records:    buildRecords(records),
	})
	if err != nil {
		panic(err)
	}
	runtime.ForwardResponseMessage(ctx, mux, outboundMarshaler, w, r, resp, mux.GetForwardResponseOptions()...)
}
