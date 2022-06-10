package hstream_http_server

import (
	hstreamApi "github.com/hstreamdb/hstream/common/api/gen-go/HStream/Server"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HostPortCtx struct {
	GRPCServerHost *string
	GRPCServerPort *string
	HttpServerPort *string
}

func buildRecord(flag hstreamApi.HStreamRecordHeader_Flag, key string, payload []byte) *hstreamApi.HStreamRecord {
	header := hstreamApi.HStreamRecordHeader{
		Flag:        flag,
		Attributes:  map[string]string{},
		PublishTime: timestamppb.Now(),
		Key:         key,
	}
	return &hstreamApi.HStreamRecord{
		Header:  &header,
		Payload: payload,
	}
}
