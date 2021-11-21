package hstream_http_server_test

import (
	"bytes"
	"flag"
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
	serverPrefix = flag.String("server-prefix", "http://0.0.0.0:6580", "")
)

func TestMain(m *testing.M) {
	flag.Parse()
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
	resp, err := http.Get(*serverPrefix + "/streams")
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
	var listResp hstreamHttpServer.ListStreamsResponse
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
	resp, err = http.Post(*serverPrefix+"/streams", "application/json", streamReader)
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
	var createResp hstreamHttpServer.Stream
	if err := protojson.Unmarshal(body1, &createResp); err != nil {
		panic(err)
	}

	resp, err = http.Get(*serverPrefix + "/streams")
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	assert.Equal(t, 200, resp.StatusCode)
	body1, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body1)
	assert.NotEqual(t, body0, body1)
	if err := protojson.Unmarshal(body1, &listResp); err != nil {
		panic(err)
	}

	const record = `{
	"x": 8,
	"y": 7,
	"Hello": "World"
}`
	recordReader := strings.NewReader(record)
	resp, err = http.Post(*serverPrefix+"/streams/test_stream:publish", "application/json", recordReader)
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
	var appendResp hstreamHttpServer.AppendResponse
	if err := protojson.Unmarshal(body1, &appendResp); err != nil {
		panic(err)
	}

	req, err := http.NewRequest(http.MethodDelete, *serverPrefix+"/streams/test_stream", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
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

	resp, err = http.Get(*serverPrefix + "/streams")
	if err != nil {
		panic(err)
	}
	defer closeBody(resp.Body)
	assert.Equal(t, 200, resp.StatusCode)
	body1, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	assert.NotEmpty(t, body1)
	assert.Equal(t, body0, body1)
	if err := protojson.Unmarshal(body1, &listResp); err != nil {
		panic(err)
	}
}

func TestView(t *testing.T) {
	resp, err := http.Get(*serverPrefix + "/views")
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
	resp, err = http.Post(*serverPrefix+"/streams", "application/json", streamReader)
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
	resp, err = http.Post(*serverPrefix+"/views",
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
	req, err := http.NewRequest(http.MethodDelete, *serverPrefix+"/views/test_view", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	resp, err = http.Get(*serverPrefix + "/views")
	body1 = execResp(t, resp, err, &listResp)
	assert.Equal(t, string(body0), string(body1))

	req, err = http.NewRequest(http.MethodDelete, *serverPrefix+"/streams/test_stream", nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)
}

func TestQuery(t *testing.T) {
	var listResp hstreamHttpServer.ListQueriesResponse
	resp, err := http.Get(*serverPrefix + "/queries")
	execResp(t, resp, err, &listResp)
}
