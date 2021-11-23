package hstream_http_server

import (
	hstreamApi "github.com/hstreamdb/hstream/common/gen-go/HStream/Server"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HostPortCtx struct {
	GRPCServerHost *string
	GRPCServerPort *string
	HttpServerPort *string
}

func buildRecord(payload []byte) *hstreamApi.HStreamRecord {
	header := hstreamApi.HStreamRecordHeader{
		Flag:        hstreamApi.HStreamRecordHeader_JSON,
		Attributes:  map[string]string{},
		PublishTime: timestamppb.Now(),
		Key:         "",
	}
	return &hstreamApi.HStreamRecord{
		Header:  &header,
		Payload: payload,
	}
}

func buildRecords(payloads [][]byte) (ret []*hstreamApi.HStreamRecord) {
	for _, payload := range payloads {
		ret = append(ret, buildRecord(payload))
	}
	return
}
