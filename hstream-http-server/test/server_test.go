package hstream_http_server_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	hstreamHttpServer "github.com/hstreamdb/hstream-http-server/hstream-api/build/proto/HStream/Server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	grpcPort       string
	httpPort       string
	mysqlPort      string
	clickhousePort string
	serverPrefix   string
)

func TestMain(m *testing.M) {
	if env := os.Getenv("SERVER_LOCAL_PORT"); env != "" {
		grpcPort = env
	} else {
		grpcPort = "6570"
	}
	if env := os.Getenv("MYSQL_LOCAL_PORT"); env != "" {
		mysqlPort = env
	} else {
		mysqlPort = "3306"
	}
	if env := os.Getenv("CLICKHOUSE_LOCAL_PORT"); env != "" {
		mysqlPort = env
	} else {
		mysqlPort = "9000"
	}
	if env := os.Getenv("HTTP_LOCAL_PORT"); env != "" {
		httpPort = env
	} else {
		httpPort = "6580"
	}
	serverPrefix = "http://0.0.0.0:" + httpPort

	code := m.Run()
	os.Exit(code)
}

func closeBody(body io.ReadCloser) {
	if err := body.Close(); err != nil {
		panic(err)
	}
}

func assertOk(t *testing.T, statusCode int) {
	assert.Equal(t, 200, statusCode)
}

func execResp(t *testing.T, resp *http.Response, err error, unmarshalVar proto.Message) []byte {
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body)
	if resp.StatusCode != 200 {
		fmt.Println(string(body))
	}
	assertOk(t, resp.StatusCode)
	if err := protojson.Unmarshal(body, unmarshalVar); err != nil {
		panic(err)
	}
	return body
}

func TestStream(t *testing.T) {
	var listResp hstreamHttpServer.ListStreamsResponse
	resp, err := http.Get(serverPrefix + "/streams")
	body0 := execResp(t, resp, err, &listResp)

	stream := hstreamHttpServer.Stream{
		StreamName:        "test_stream",
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	var createResp hstreamHttpServer.Stream
	resp, err = http.Post(serverPrefix+"/streams", "application/json", streamReader)
	execResp(t, resp, err, &createResp)

	resp, err = http.Get(serverPrefix + "/streams")
	body1 := execResp(t, resp, err, &listResp)
	assert.NotEqual(t, body0, body1)

	const record = `{
	"x": 8,
	"y": 7,
	"Hello": "World"
}`
	recordReader := strings.NewReader(record)
	var appendResp hstreamHttpServer.AppendResponse
	resp, err = http.Post(serverPrefix+"/streams/test_stream:publish", "application/json", recordReader)
	execResp(t, resp, err, &appendResp)

	var deleteResp emptypb.Empty
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/streams/test_stream", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	resp, err = http.Get(serverPrefix + "/streams")
	body1 = execResp(t, resp, err, &listResp)
	assert.Equal(t, body0, body1)
}

func TestView(t *testing.T) {
	resp, err := http.Get(serverPrefix + "/views")
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	assertOk(t, resp.StatusCode)
	body0, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body0)
	var listResp hstreamHttpServer.ListViewsResponse
	if err := protojson.Unmarshal(body0, &listResp); err != nil {
		panic(err)
	}

	stream := hstreamHttpServer.Stream{
		StreamName:        "test_stream",
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	resp, err = http.Post(serverPrefix+"/streams", "application/json", streamReader)
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	assertOk(t, resp.StatusCode)
	body1, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body1)
	var createResp_ hstreamHttpServer.Stream
	if err := protojson.Unmarshal(body1, &createResp_); err != nil {
		panic(err)
	}
	var sql = `{
	"sql": "CREATE VIEW test_view AS SELECT x, SUM(x) FROM test_stream GROUP BY y EMIT CHANGES;"
}`
	resp, err = http.Post(serverPrefix+"/views",
		"application/json",
		strings.NewReader(sql))
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	assertOk(t, resp.StatusCode)
	body1, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body1)
	var createResp hstreamHttpServer.View
	if err := protojson.Unmarshal(body1, &createResp); err != nil {
		panic(err)
	}
	assert.NotEqual(t, string(body0), string(body1))

	var deleteResp emptypb.Empty
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/views/test_view", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	resp, err = http.Get(serverPrefix + "/views")
	body1 = execResp(t, resp, err, &listResp)
	assert.Equal(t, string(body0), string(body1))

	req, err = http.NewRequest(http.MethodDelete, serverPrefix+"/streams/test_stream", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)
}

func TestQuery(t *testing.T) {
	var listResp hstreamHttpServer.ListQueriesResponse
	resp, err := http.Get(serverPrefix + "/queries")
	execResp(t, resp, err, &listResp)

	//body0 := execResp(t, resp, err, &listResp)
	//
	//var createResp hstreamHttpServer.Query
	//createReq := hstreamHttpServer.CreateQueryRequest{
	//	Id:        "test_query",
	//	QueryText: "SELECT * FROM test_stream EMIT CHANGES;",
	//}
	//createReq_, err := protojson.Marshal(&createReq)
	//fmt.Println(string(createReq_))
	//if err != nil {
	//	panic(err)
	//}
	//resp, err = http.Post(*erverPrefix+"/v0/queries/pull", "application/json", bytes.NewReader(createReq_))
	//execResp(t, resp, err, &createResp)
	//
	//resp, err = http.Get(*erverPrefix + "/v0/queries")
	//body1 := execResp(t, resp, err, &listResp)
	//assert.NotEqual(t, body0, body1)
	//
	//terminateReq := hstreamHttpServer.TerminateQueriesRequest{
	//	QueryId: []string{"test_query"},
	//	All:     false,
	//}
	//terminateReq_, err := protojson.Marshal(&terminateReq)
	//if err != nil {
	//	panic(err)
	//}
	//var terminateResp hstreamHttpServer.TerminateQueriesResponse
	//resp, err = http.Post(*erverPrefix+"/v0/queries/terminate", "application/json", bytes.NewReader(terminateReq_))
	//execResp(t, resp, err, &terminateResp)
	//
	//var deleteResp emptypb.Empty
	//req, err := http.NewRequest(http.MethodDelete, *erverPrefix+"/v0/queries/test_query", nil)
	//if err != nil {
	//	panic(err)
	//}
	//resp, err = http.DefaultClient.Do(req)
	//execResp(t, resp, err, &deleteResp)
	//
	//resp, err = http.Get(*erverPrefix + "/v0/queries")
	//body1 = execResp(t, resp, err, &listResp)
	//assert.Equal(t, body0, body1)
}

func TestConnector(t *testing.T) {
	stream := hstreamHttpServer.Stream{
		StreamName:        "test_stream",
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	resp, err := http.Post(serverPrefix+"/streams", "application/json", streamReader)
	var createResp_ hstreamHttpServer.Stream
	execResp(t, resp, err, &createResp_)

	createReq := hstreamHttpServer.CreateSinkConnectorRequest{
		Sql: "CREATE SINK CONNECTOR test_connector WITH (type=mysql, host=\"127.0.0.1\", port=" + mysqlPort + ", username=\"root\", password=\"\", database=\"mysql\", stream=test_stream);",
	}
	connectorByte, err := protojson.Marshal(&createReq)
	if err != nil {
		panic(err)
	}
	connectorReader := bytes.NewReader(connectorByte)
	resp, err = http.Post(serverPrefix+"/connectors", "application/json", connectorReader)
	var createResp hstreamHttpServer.Connector
	execResp(t, resp, err, &createResp)

	var terminateResp emptypb.Empty
	resp, err = http.Post(serverPrefix+"/connectors/test_connector:terminate", "application/json", bytes.NewReader([]byte{}))
	execResp(t, resp, err, &terminateResp)

	var deleteResp hstreamHttpServer.DeleteConnectorResponse
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
