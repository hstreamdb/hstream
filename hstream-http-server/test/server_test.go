package hstream_http_server_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"
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
		mysqlPort = "34049"
	}
	if env := os.Getenv("HTTP_LOCAL_PORT"); env != "" {
		httpPort = env
	} else {
		httpPort = "6580"
	}
	serverPrefix = "http://0.0.0.0:" + httpPort

	rand.Seed(time.Now().UnixNano())
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

func randText(n int) string {
	rand.Seed(time.Now().UnixNano())
	var xs = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	ret := make([]rune, n)
	for i := range ret {
		ret[i] = xs[rand.Intn(len(xs))]
	}
	return string(ret)
}
