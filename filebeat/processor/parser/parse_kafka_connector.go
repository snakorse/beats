package parser

import (
	"regexp"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/processors"
	"github.com/pkg/errors"
)

const kafkaConnectionNameMust = "addon-kafka-connect"

type parseKafkaConnector struct {
	regex *regexp.Regexp
}

func init() {
	processors.RegisterPlugin("parse_kafka_connector", newParseKafkaConnector)
}

func newParseKafkaConnector(c *common.Config) (processors.Processor, error) {
	logp.Debug("parse_kafka_connector", "new parse kafka connector processor")

	regex, err := regexp.Compile("\\[(.+)] .* .*{id=(.+)-\\d+}")
	if err != nil {
		return nil, err
	}

	return &parseKafkaConnector{regex: regex}, nil
}

// [2020-06-11 09:09:29,846] ERROR WorkerConnector{id=mysql_source_zhengzhi_test} Connector raised an error (org.apache.kafka.connect.runtime.WorkerConnector:91)
func (p *parseKafkaConnector) Run(event *beat.Event) (*beat.Event, error) {
	logp.Debug("parse_kafka_connector", "start parse kafka connector message")

	message, err := event.GetValue("message")
	if err != nil {
		return event, errors.Wrap(err, "fail to get message value")
	}

	// not kafka-connection container
	if name, err := event.GetValue("docker.container.name"); name != "" && err == nil &&
		!strings.Contains(name.(string), kafkaConnectionNameMust) {
		return event, nil
	}

	logp.Debug("parse_kafka_connector", "parse kafka connector message: %s", message)
	matches := p.regex.FindStringSubmatch(message.(string))
	if len(matches) >= 3 {
		timespec := matches[1]
		timespec = strings.Replace(timespec, ",", ".", -1)
		ts, err := time.Parse("2006-01-02 15:04:05.000", timespec)
		if err != nil {
			return event, errors.Errorf("fail to parse timestamp: %s", timespec)
		}

		if matches[2] == "" {
			return event, errors.New("fail to get id value")
		}

		event.PutValue("terminus.source", "kafka-connector")
		event.PutValue("terminus.id", matches[2])
		event.PutValue("@timestamp", ts)
	}

	logp.Debug("parse_kafka_connector", "parse kafka connector matches: %v", matches)
	return event, nil
}

func (*parseKafkaConnector) String() string {
	return "parse_kafka_connector"
}
