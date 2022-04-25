package hstream_http_server_test

import (
	"bytes"
	"fmt"
	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net/http"
	"testing"
	"time"
)

func xTestQuery(t *testing.T) {
	test_stream := randText(8)
	test_query := randText(8)

	stream := hstreamApi.Stream{
		StreamName:        test_stream,
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	resp, err := http.Post(serverPrefix+"/streams", "application/json", streamReader)
	var createResp_ hstreamApi.Stream
	execResp(t, resp, err, &createResp_)
	var listResp hstreamApi.ListQueriesResponse
	resp, err = http.Get(serverPrefix + "/queries")
	body0 := execResp(t, resp, err, &listResp)

	createReq := hstreamApi.CreateQueryRequest{
		Id:        test_query,
		QueryText: "SELECT * FROM " + test_stream + " EMIT CHANGES;",
	}
	createReq_, err := protojson.Marshal(&createReq)
	fmt.Println(string(createReq_))
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(5 * time.Second)
		resp, err = http.Get(serverPrefix + "/queries")
		body1 := execResp(t, resp, err, &listResp)
		assert.NotEqual(t, body0, body1)
		terminateReq := hstreamApi.TerminateQueriesRequest{
			QueryId: []string{test_query},
			All:     false,
		}
		terminateReq_, err := protojson.Marshal(&terminateReq)
		if err != nil {
			panic(err)
		}
		var terminateResp hstreamApi.TerminateQueriesResponse
		resp, err = http.Post(serverPrefix+"/queries/terminate", "application/json", bytes.NewReader(terminateReq_))
		execResp(t, resp, err, &terminateResp)

		var deleteResp emptypb.Empty
		req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/queries/"+test_query, nil)
		if err != nil {
			panic(err)
		}
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Println(err)
		}

		req, err = http.NewRequest(http.MethodDelete, serverPrefix+"/streams/"+test_stream, nil)
		if err != nil {
			panic(err)
		}
		resp, err = http.DefaultClient.Do(req)
		execResp(t, resp, err, &deleteResp)
	}()
	var deleteResp emptypb.Empty
	resp, err = http.Post(serverPrefix+"/queries/push", "application/json", bytes.NewReader(createReq_))
	time.Sleep(5 * time.Second)
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/streams/"+test_stream+"?ignoreNonExist=true", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)
}
