package hstream_http_server_test

import (
	"bytes"
	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net/http"
	"strings"
	"testing"
)

func TestStream(t *testing.T) {
	test_stream := randText(8)
	var listResp hstreamApi.ListStreamsResponse
	resp, err := http.Get(serverPrefix + "/streams")
	body0 := execResp(t, resp, err, &listResp)

	stream := hstreamApi.Stream{
		StreamName:        test_stream,
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	var createResp hstreamApi.Stream
	resp, err = http.Post(serverPrefix+"/streams", "application/json", streamReader)
	execResp(t, resp, err, &createResp)

	resp, err = http.Get(serverPrefix + "/streams")
	body1 := execResp(t, resp, err, &listResp)
	assert.NotEqual(t, body0, body1)
	assert.NotContains(t, string(body0), test_stream)
	assert.Contains(t, string(body1), test_stream)

	const record = `{"flag": 0, "payload": "{\"x\": 8, \"y\": 7, \"Hello\": \"World\"}"}`
	recordReader := strings.NewReader(record)
	var appendResp hstreamApi.AppendResponse
	resp, err = http.Post(serverPrefix+"/streams/"+test_stream+":publish", "application/json", recordReader)
	execResp(t, resp, err, &appendResp)

	var deleteResp emptypb.Empty
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/streams/"+test_stream, nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	resp, err = http.Get(serverPrefix + "/streams")
	body1 = execResp(t, resp, err, &listResp)
	assert.NotContains(t, string(body1), test_stream)
}
