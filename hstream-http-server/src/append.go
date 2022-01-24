package hstream_http_server

import (
	"context"
	"encoding/json"
	"fmt"

	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"

	"log"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"google.golang.org/grpc"
)

type B64Payload struct {
	Flag    hstreamApi.HStreamRecordHeader_Flag
	Payload string `json:"payload"`
	Key     string `json:"key"`
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
	_, outboundMarshaller := runtime.MarshalerForRequest(mux, r)

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		log.Printf("Error: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad Request Error: Invalid append payload.\n" + fmt.Sprint(err)))
		return
	}

	conn, err := grpc.Dial(*hpCtx.GRPCServerHost+":"+*hpCtx.GRPCServerPort,
		grpc.WithInsecure())
	if err != nil {
		log.Printf("Error: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}
	defer func(conn *grpc.ClientConn) {
		if err := conn.Close(); err != nil {

			log.Printf("Error: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}
	}(conn)
	c := hstreamApi.NewHStreamApiClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := c.Append(ctx, &hstreamApi.AppendRequest{
		StreamName: pathParams["streamName"],
		Records:    []*hstreamApi.HStreamRecord{buildRecord(in.Flag, in.Key, []byte(in.Payload))},
	})
	if err != nil {
		log.Printf("Error: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}
	runtime.ForwardResponseMessage(ctx, mux, outboundMarshaller, w, r, resp, mux.GetForwardResponseOptions()...)
}
