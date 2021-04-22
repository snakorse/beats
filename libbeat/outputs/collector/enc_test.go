// Copyright (c) 2021 Terminus, Inc.

// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.

// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.

// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package collector

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io/ioutil"
	"testing"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/stretchr/testify/assert"
)

func TestGzipEncode(t *testing.T) {
	genc, err := newGzipEncoder(9)
	assert.Nil(t, err)

	b, err := genc.encode(mockEvent())

	assert.Nil(t, err)
	gr, err := gzip.NewReader(bytes.NewReader(b.Bytes()))
	br := base64.NewDecoder(base64.StdEncoding, gr)
	o, err := ioutil.ReadAll(br)
	assert.Equal(t, `[{"content":"\u001b[37mDEBU\u001b[0m[2021-04-22 14:18:52.265950181] finished handle request GET /health (took 107.411µs) ","id":"77e90e85233cb3ec1bfd7655633248056a3fc03e092596c405a5b158cc8885b2","labels":{},"offset":17420730,"source":"container","stream":"stdout","tags":{"container_name":"qa","dice_cluster_name":"terminus-dev","dice_component":"qa","pod_name":"dice-qa-7cb5b7fd4-494zb","pod_namespace":"default"},"timestamp":1415792726371000000}]`, string(o))
}

func mockEvent() []publisher.Event {
	t, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-11-12T11:45:26.371Z")

	res := []publisher.Event{
		{
			Content: beat.Event{
				Timestamp: t,
				Meta:      nil,
				Fields: common.MapStr{
					"terminus": common.MapStr{
						"tags": common.MapStr{
							"DICE_CLUSTER_NAME": "terminus-dev",
							"pod_name":          "dice-qa-7cb5b7fd4-494zb",
							"pod_namespace":     "default",
							"container_name":    "qa",
							"DICE_COMPONENT":    "qa",
						},
						"labels": common.MapStr{},
						"id":     "77e90e85233cb3ec1bfd7655633248056a3fc03e092596c405a5b158cc8885b2",
						"source": "container",
					},
					"log": common.MapStr{
						"offset": 17420730,
						"file": common.MapStr{
							"path": "/var/lib/docker/containers/77e90e85233cb3ec1bfd7655633248056a3fc03e092596c405a5b158cc8885b2/77e90e85233cb3ec1bfd7655633248056a3fc03e092596c405a5b158cc8885b2-json.log",
						},
					},
					"message": "\u001b[37mDEBU\u001b[0m[2021-04-22 14:18:52.265950181] finished handle request GET /health (took 107.411µs) ",
					"stream": "stdout",
				},
				Private:    nil,
				TimeSeries: false,
			},
			Flags: publisher.GuaranteedSend,
			Cache: publisher.EventCache{},
		},
	}
	return res
}


