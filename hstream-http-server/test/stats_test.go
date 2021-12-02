package hstream_http_server_test

import (
	"bytes"
	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"
	"google.golang.org/protobuf/encoding/protojson"
	"net/http"
	"testing"
)

func TestStats(t *testing.T) {
	statsAllReq := hstreamApi.PerStreamTimeSeriesStatsAllRequest{
		Method:    "appends",
		Intervals: &hstreamApi.StatsIntervalVals{Intervals: []int32{10000}},
	}
	req, err := protojson.Marshal(&statsAllReq)
	if err != nil {
		panic(err)
	}
	var statsAllResp hstreamApi.PerStreamTimeSeriesStatsAllResponse
	resp, err := http.Post(serverPrefix+"/stats", "application/json", bytes.NewReader(req))
	execResp(t, resp, err, &statsAllResp)
}
