package hstream_http_server_test

import (
	"bytes"
	hstreamApi "github.com/hstreamdb/hstream/common/api/gen-go/HStream/Server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net/http"
	"strings"
	"testing"
)

func TestView(t *testing.T) {
	test_stream := randText(8)
	test_view := randText(8)
	var listResp hstreamApi.ListViewsResponse
	resp, err := http.Get(serverPrefix + "/views")
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
	resp, err = http.Post(serverPrefix+"/streams", "application/json", streamReader)
	var createResp_ hstreamApi.Stream
	body1 := execResp(t, resp, err, &createResp_)

	var sql = "{\"sql\": \"CREATE VIEW " + test_view + " AS SELECT x, SUM(x) FROM " + test_stream + " GROUP BY y EMIT CHANGES;\"}"
	resp, err = http.Post(serverPrefix+"/views",
		"application/json",
		strings.NewReader(sql))
	var createResp hstreamApi.View
	body1 = execResp(t, resp, err, &createResp)
	assert.NotEqual(t, string(body0), string(body1))

	var deleteResp emptypb.Empty
	req, err := http.NewRequest(http.MethodDelete, serverPrefix+"/views/"+test_view, nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)

	resp, err = http.Get(serverPrefix + "/views")
	body1 = execResp(t, resp, err, &listResp)
	assert.NotContains(t, string(body1), test_view)

	req, err = http.NewRequest(http.MethodDelete, serverPrefix+"/streams/"+test_stream, nil)
	if err != nil {
		panic(err)
	}
	resp, err = http.DefaultClient.Do(req)
	execResp(t, resp, err, &deleteResp)
}
