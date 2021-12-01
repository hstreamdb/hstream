package hstream_http_server_test

import (
	"bytes"
	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net/http"
	"testing"
)

func TestConnector(t *testing.T) {
	stream := hstreamApi.Stream{
		StreamName:        "test_stream",
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

	createReq := hstreamApi.CreateSinkConnectorRequest{
		Sql: "CREATE SINK CONNECTOR test_connector WITH (type=mysql, host=\"127.0.0.1\", port=" + mysqlPort + ", username=\"root\", password=\"\", database=\"mysql\", stream=test_stream);",
	}
	connectorByte, err := protojson.Marshal(&createReq)
	if err != nil {
		panic(err)
	}
	connectorReader := bytes.NewReader(connectorByte)
	resp, err = http.Post(serverPrefix+"/connectors", "application/json", connectorReader)
	var createResp hstreamApi.Connector
	execResp(t, resp, err, &createResp)

	var terminateResp emptypb.Empty
	resp, err = http.Post(serverPrefix+"/connectors/test_connector:terminate", "application/json", bytes.NewReader([]byte{}))
	execResp(t, resp, err, &terminateResp)

	var deleteResp hstreamApi.DeleteConnectorResponse
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/connectors/test_connector", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	req, err = http.NewRequest(http.MethodDelete, serverPrefix+"/streams/test_stream", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)
}
