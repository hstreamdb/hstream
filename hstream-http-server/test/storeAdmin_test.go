package hstream_http_server_test

import (
	hstreamApi "github.com/hstreamdb/hstream/common/api/gen-go/HStream/Server"
	"net/http"
	"testing"
)

func TestNode(t *testing.T) {
	var listResp hstreamApi.ListNodesResponse
	resp, err := http.Get(serverPrefix + "/nodes")
	execResp(t, resp, err, &listResp)

	var getResp hstreamApi.Node
	resp, err = http.Get(serverPrefix + "/nodes/0")
	execResp(t, resp, err, &getResp)
}
