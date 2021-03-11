package parser

import (
	"fmt"
	"os"
	"strings"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/processors"
	"github.com/pkg/errors"
)

type parseKubeApiserverAudit struct {
	fileName string
	cluster  string
}

func init() {
	processors.RegisterPlugin("parse_kube_apiserver_audit", newParseKubeApiserverAudit)
}

func newParseKubeApiserverAudit(c *common.Config) (processors.Processor, error) {
	logp.Debug("parse_kube_apiserver_audit", "new parse kube apiserver audit processor")

	config := struct {
		FileName   string `config:"file_name"`
		ClusterKey string `config:"cluster_key"`
	}{}
	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("fail to unpack the parse_kube_apiserver_audit configuration: %s", err)
	}

	cluster := os.Getenv(config.ClusterKey)
	return &parseKubeApiserverAudit{
		fileName: config.FileName,
		cluster:  cluster,
	}, nil
}

func (p *parseKubeApiserverAudit) Run(event *beat.Event) (*beat.Event, error) {
	logp.Debug("parse_kube_apiserver_audit", "start parse kube apiserver audit message")

	if p.cluster == "" {
		return event, nil
	}

	value, err := event.GetValue("log.file.path")
	if err != nil {
		return event, errors.Wrap(err, "ail to get source field")
	}
	source, ok := value.(string)
	if !ok {
		return event, errors.Wrap(err, "source field is not string")
	}
	if strings.Contains(source, p.fileName) {
		event.Fields.Put("terminus.source", "kube-apiserver-audit")
		event.Fields.Put("terminus.id", p.cluster)
	}
	return event, nil
}

func (*parseKubeApiserverAudit) String() string {
	return "parse_kube_apiserver_audit"
}
