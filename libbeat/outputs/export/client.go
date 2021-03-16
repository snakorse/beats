package export

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	netUrl "net/url"
	"time"

	"github.com/elastic/beats/v7/libbeat/common/transport"
	"github.com/elastic/beats/v7/libbeat/common/transport/tlscommon"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type client struct {
	enc                 encoder
	outputCompressLevel int
	outputClient        *http.Client
	outputParams        map[string]string
	outputHeaders       map[string]string
	outputMethod        string
	outputHost          string
	observer            outputs.Observer
}

func newClient(host string, cfg config, observer outputs.Observer) (*client, error) {
	var (
		enc encoder
		err error
	)
	if cfg.CompressLevel == 0 {
		enc, err = newJSONEncoder()
	} else {
		enc, err = newGzipEncoder(cfg.CompressLevel)
	}
	if err != nil {
		return nil, errors.Wrap(err, "fail to create encoder")
	}

	// 创建日志导出client
	outputTLS, err := tlscommon.LoadTLSConfig(cfg.TLS)
	if err != nil {
		return nil, errors.Wrap(err, "fail to load output tls")
	}
	outputClient, err := newHTTPClient(cfg.Timeout, cfg.KeepAlive, outputTLS, observer)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create output client")
	}

	return &client{
		enc:                 enc,
		outputCompressLevel: cfg.CompressLevel,
		outputClient:        outputClient,
		outputMethod:        cfg.Method,
		outputParams:        cfg.Params,
		outputHeaders:       cfg.Headers,
		outputHost:          host,
		observer:            observer,
	}, nil
}

func newRequest(host, path, method string, params, headers map[string]string) (*http.Request, error) {
	values := netUrl.Values{}
	for key, value := range params {
		values.Add(key, value)
	}
	url := host + path + "?" + values.Encode()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create request")
	}

	splitHost, _, err := net.SplitHostPort(req.Host)
	if err == nil {
		req.Host = splitHost
	}

	for key, value := range headers {
		req.Header.Add(key, value)
	}
	req.Header.Add("Accept", "application/json")
	return req, nil
}

func newHTTPClient(
	timeout, keepAlive time.Duration,
	tls *tlscommon.TLSConfig,
	observer outputs.Observer,
) (*http.Client, error) {
	dialer := transport.NetDialer(timeout)
	tlsDialer, err := transport.TLSDialer(dialer, tls, timeout)
	if err != nil {
		return nil, errors.Wrap(err, "fail to create tls dialer")
	}

	dialer = transport.StatsDialer(dialer, observer)
	tlsDialer = transport.StatsDialer(tlsDialer, observer)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: keepAlive,
				DualStack: true,
			}).DialContext,
			Dial:    dialer.Dial,
			DialTLS: tlsDialer.Dial,
		},
	}
	return client, nil
}

func (c *client) Connect() error {
	return nil
}

func (c *client) Close() error {
	return nil
}

func (c *client) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	rest, err := c.publishEvents(events)
	c.observer.NewBatch(len(events))
	if len(rest) == 0 {
		batch.ACK()
	} else {
		c.observer.Failed(len(rest))
		batch.RetryEvents(rest)
	}
	return err
}

func (c *client) publishEvents(events []publisher.Event) ([]publisher.Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	err := c.sendOutputEvents(events)
	if err != nil {
		return events, err
	}
	return nil, nil
}

func (c *client) sendOutputEvents(events []publisher.Event) error {
	return c.sendOutputAddrEvents(c.outputHost, events)
}

func (c *client) sendOutputAddrEvents(addr string, events []publisher.Event) error {
	// 避免共用主协程的gzip，每次发送都创建enc
	var (
		enc encoder
		err error
	)
	if c.outputCompressLevel == 0 {
		enc, err = newJSONEncoder()
	} else {
		enc, err = newGzipEncoder(c.outputCompressLevel)
	}
	if err != nil {
		return fmt.Errorf("fail to create encoder: %s", err)
	}

	body, err := enc.encode(events)
	if err != nil {
		return fmt.Errorf("fail to encode output %s events: %s", addr, err)
	}
	now := time.Now().UnixNano()

	req, err := newRequest(addr, "", c.outputMethod, c.outputParams, c.outputHeaders)
	if err != nil {
		return fmt.Errorf("fail to create output request %s", addr)
	}
	enc.addHeader(&req.Header)
	var requestID string
	if key, err := uuid.NewV4(); err == nil {
		requestID = key.String()
	}
	req.Header.Set("terminus-request-id", requestID)

	req.Body = ioutil.NopCloser(body)
	resp, err := c.outputClient.Do(req)
	if err != nil {
		return fmt.Errorf("fail to send %s output: %s", addr, err)
	}
	defer closeResponseBody(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("output %s response status is not success, code is %v", addr, resp.StatusCode)
	}

	logp.Info("send output %s request %s success, count: %v, cost: %.3fs",
		addr, requestID, len(events), float64(time.Now().UnixNano()-now)/float64(time.Second))
	return nil
}

func closeResponseBody(body io.ReadCloser) {
	if err := body.Close(); err != nil {
		logp.Warn("fail to close response body. err: %s", err)
	}
}

func (c *client) String() string {
	return outputName
}
