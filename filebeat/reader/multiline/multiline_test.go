// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// +build !integration

package multiline

import (
	"bytes"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/beats/v7/libbeat/common/match"
	"github.com/elastic/beats/v7/libbeat/reader"
	"github.com/elastic/beats/v7/libbeat/reader/readfile"
	"github.com/elastic/beats/v7/libbeat/reader/readfile/encoding"
)

type bufferSource struct{ buf *bytes.Buffer }

func (p bufferSource) Read(b []byte) (int, error) { return p.buf.Read(b) }
func (p bufferSource) Close() error               { return nil }
func (p bufferSource) Name() string               { return "buffer" }
func (p bufferSource) Stat() (os.FileInfo, error) { return nil, errors.New("unknown") }
func (p bufferSource) Continuable() bool          { return false }

// func TestMultilineAfterOK(t *testing.T) {
// 	pattern := match.MustCompile(`^[ \t] +`) // next line is indented by spaces
// 	testMultilineOK(t,
// 		Config{
// 			Patterns: []match.Matcher{pattern, match.MustCompile(`ddd`)},
// 			Match:    "after",
// 		},
// 		2,
// 		"line1\n  line1.1\n  line1.2\n",
// 		"line2\n  line2.1\n  line2.2\n",
// 	)
// }

func TestMultilineNotMatchAll(t *testing.T) {
	pattern := match.MustCompile(`^aaa$`)
	testMultilineOK(t,
		Config{
			Patterns: []match.Matcher{pattern},
			Negate:   false,
			Match:    "after",
		},
		3,
		"bbb\n",
		"ccc\n",
		"ddd\n",
	)
}

func TestMultilineMatchNormal1(t *testing.T) {
	pattern := match.MustCompile(`^aaa$`)
	testMultilineOK(t,
		Config{
			Patterns: []match.Matcher{pattern},
			Negate:   false,
			Match:    "after",
		},
		3,
		"aaa\nccc\n",
		"aaa\nddd\n",
		"aaa\n",
	)
}

func TestMultilineMatchNormal2(t *testing.T) {
	pattern := match.MustCompile(`^aaa$`)
	testMultilineOK(t,
		Config{
			Patterns: []match.Matcher{pattern},
			Negate:   false,
			Match:    "after",
		},
		4,
		"ccc\n",
		"aaa\nddd\n",
		"aaa\n",
		"aaa\neee\n",
	)
}

// 匹配所有可能的日志起始行
func TestMultilineMatch(t *testing.T) {
	may := []string{
		`time="2020-06-22T17:14:35+08:00" level=info msg="found spec: &{Path:/api/orgs/actions/get-by-domain BackendPath:/api/orgs/actions/get-by-domain Host:cmdb.marathon.l4lb.thisdcos.directory:9093 Scheme:http Method:GET Custom:<nil> CustomResponse:<nil> CheckLogin:false TryCheckLogin:true CheckToken:true CheckBasicAuth:false ChunkAPI:false MarathonHost:cmdb.marathon.l4lb.thisdcos.directory K8SHost:cmdb.default.svc.cluster.local Port:9093}"`,
		`2020-06-22 17:13:55,808 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - aggregate renderer -> Map -> Sink: send alert message to eventbox (1/2) (b1fb8a63d7a2e52cf43ec1c6a5facb9f) switched from DEPLOYING to RUNNING.`,
		`2020-06-22T17:14:36.699+0800    INFO    log/input.go:576        File is falling under ignore_older before harvesting is finished. Adjust your close_* settings: /var/lib/docker/containers/12b6258bec9fa98ca5f7a0dff99f63a7b4ec22fad64d1f5b6e072003f4dbb1a0/12b6258bec9fa98ca5f7a0dff99f63a7b4ec22fad64d1f5b6e072003f4dbb1a0-json.log`,
		`2020-06-29 14:35:59.938 ERROR [trade-starter] ---  --- [ConsumeMessageThread_7] i.t.p.t.m.listener.TradeMessageListener  : failed to consume message,message tag:CRM_CUSTOMER_SETTLEMENT_TAGS,id:AC1602AB000E10D5928693327A8E0008,payload:{"shopId":"2100038003","customerId":"10074","shopFreezeStatus":false,"tradeType":false,"settlementIntervalType":3,"settlementInterval":914080805347,"settlementIntervalStartTime":"2020-06-29 13:59:18"},cause:io.terminus.parana.trade.common.ecxception.TradeException: Trade message consumer with tag [CRM_CUSTOMER_SETTLEMENT_TAGS] not exists.`,
		`[INFO] Downloading from terminus: http://addon-nexus.default.svc.cluster.local:8081/repository/public/org/apache/maven/shared/maven-dependency-tree/3.0.1/maven-dependency-tree-3.0.1.pom`,
	}

	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(\W*)(\d{4}-\d{2}-\d{2}(\s|T)\d{2}:\d{2}:\d{2})(.*?)([Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|DEBU|[Nn]otice|NOTICE|[Ii]nfo|INFO|[Ww]arn(?:ing)?|WARN(?:ING)?|[Ee]rr(?:or)?|ERR(?:OR)?|[Cc]rit(?:ical)?|CRIT(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|[Ee]merg(?:ency)?|EMERG(?:ENCY)?)(.*?)`),
		regexp.MustCompile(`\[([Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|DEBU|[Nn]otice|NOTICE|[Ii]nfo|INFO|[Ww]arn(?:ing)?|WARN(?:ING)?|[Ee]rr(?:or)?|ERR(?:OR)?|[Cc]rit(?:ical)?|CRIT(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|[Ee]merg(?:ency)?|EMERG(?:ENCY)?)\](.*?)`),
	}

	for i, s := range may {
		matched := false
		for _, p := range patterns {
			if p.Match([]byte(s)) {
				matched = true
				// t.Logf("line <%d> matche with pattern <%d>\n", i, j)
				break
			}
		}
		if !matched {
			t.Errorf("line <%d> not matched\n", i)
		}
	}

}

func testMultilineOK(t *testing.T, cfg Config, events int, expected ...string) {
	_, buf := createLineBuffer(expected...)
	r := createMultilineTestReader(t, buf, cfg)

	var messages []reader.Message
	for {
		message, err := r.Next()
		if err != nil {
			break
		}

		messages = append(messages, message)
	}

	if len(messages) != events {
		t.Fatalf("expected %v lines, read only %v line(s)", len(expected), len(messages))
	}

	for i, message := range messages {
		var tsZero time.Time

		assert.NotEqual(t, tsZero, message.Ts)
		assert.Equal(t, strings.TrimRight(expected[i], "\r\n "), string(message.Content))
		assert.Equal(t, len(expected[i]), int(message.Bytes))
	}
}

func createMultilineTestReader(t *testing.T, in *bytes.Buffer, cfg Config) reader.Reader {
	encFactory, ok := encoding.FindEncoding("plain")
	if !ok {
		t.Fatalf("unable to find 'plain' encoding")
	}

	enc, err := encFactory(in)
	if err != nil {
		t.Fatalf("failed to initialize encoding: %v", err)
	}

	var r reader.Reader
	r, err = readfile.NewEncodeReader(in, readfile.Config{
		Codec:      enc,
		BufferSize: 4096,
		Terminator: readfile.LineFeed,
	})
	if err != nil {
		t.Fatalf("Failed to initialize line reader: %v", err)
	}

	r, err = New(readfile.NewStripNewline(r, readfile.LineFeed), "\n", 1<<20, &cfg)
	if err != nil {
		t.Fatalf("failed to initialize reader: %v", err)
	}

	return r
}

func createLineBuffer(lines ...string) ([]string, *bytes.Buffer) {
	buf := bytes.NewBuffer(nil)
	for _, line := range lines {
		buf.WriteString(line)
	}
	return lines, buf
}
