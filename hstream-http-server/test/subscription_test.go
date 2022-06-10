package hstream_http_server_test

import (
	"bytes"
	hstreamApi "github.com/hstreamdb/hstream/common/api/gen-go/HStream/Server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	"net/http"
	"testing"
)

func TestSubscription(t *testing.T) {
	testStreamName := randText(8)
	stream := hstreamApi.Stream{
		StreamName:        testStreamName,
		ReplicationFactor: 3,
	}
	streamByte, err := protojson.Marshal(&stream)
	if err != nil {
		panic(err)
	}
	streamReader := bytes.NewReader(streamByte)
	resp, err := http.Post(serverPrefix+"/streams", "application/json", streamReader)
	execResp(t, resp, err, nil)

	testSubscriptionName := randText(8)
	subscription := hstreamApi.Subscription{
		SubscriptionId:    testSubscriptionName,
		StreamName:        testStreamName,
		AckTimeoutSeconds: 5,
	}
	subscriptionByte, err := protojson.Marshal(&subscription)
	if err != nil {
		panic(err)
	}
	subscriptionReader := bytes.NewReader(subscriptionByte)
	resp, err = http.Post(serverPrefix+"/subscriptions", "application/json", subscriptionReader)
	var createResp hstreamApi.Subscription
	execResp(t, resp, err, &createResp)
	assert.Equal(t, createResp.SubscriptionId, testSubscriptionName)

	resp, err = http.Get(serverPrefix + "/clusters/subscriptions/" + testSubscriptionName)
	var lookupResp hstreamApi.LookupSubscriptionResponse
	execResp(t, resp, err, &lookupResp)
	assert.Equal(t, testSubscriptionName, lookupResp.SubscriptionId)
	assert.NotNil(t, lookupResp.ServerNode)
}
