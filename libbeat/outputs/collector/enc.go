package collector

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/pkg/errors"
)

type encoder interface {
	addHeader(*http.Header)
	encode([]publisher.Event) (*bytes.Buffer, error)
}

type jsonEncoder struct{}

func newJSONEncoder() (*jsonEncoder, error) {
	return &jsonEncoder{}, nil
}

func (e *jsonEncoder) addHeader(header *http.Header) {
	header.Add("Content-Type", "application/json; charset=UTF-8")
	header.Add("Custom-Content-Encoding", "base64")
}

func (e *jsonEncoder) encode(obj []publisher.Event) (*bytes.Buffer, error) {
	events := make([]map[string]interface{}, 0)
	for _, o := range obj {
		m, err := transformMap(o)
		if err != nil {
			logp.Err("Fail to transform map with err: %s", err)
			continue
		}
		if m == nil { // ignore
			continue
		}
		events = append(events, m)
	}

	data, err := json.Marshal(events)
	if err != nil {
		return nil, errors.Wrap(err, "fail to json marshal events")
	}

	buffer := &bytes.Buffer{}
	encoder := base64.NewEncoder(base64.StdEncoding, buffer)
	if _, err := encoder.Write(data); err != nil {
		return nil, errors.Wrap(err, "fail to base64 encode data")
	}
	encoder.Close()
	return buffer, nil
}

type gzipEncoder struct {
	buf    *bytes.Buffer
	writer *gzip.Writer
}

func newGzipEncoder(level int) (*gzipEncoder, error) {
	buf := bytes.NewBuffer(nil)
	writer, err := gzip.NewWriterLevel(buf, level)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create gzip level %v writer", level)
	}

	return &gzipEncoder{
		buf:    buf,
		writer: writer,
	}, nil
}

func (ge *gzipEncoder) addHeader(header *http.Header) {
	header.Add("Content-Type", "application/json; charset=UTF-8")
	header.Add("Content-Encoding", "gzip")
	header.Add("Custom-Content-Encoding", "base64")
}

func (ge *gzipEncoder) encode(obj []publisher.Event) (*bytes.Buffer, error) {
	var (
		data []byte
		err  error
	)
	buffer := &bytes.Buffer{}
	defer func() {
		ge.buf.Reset()
		ge.writer.Reset(ge.buf)
	}()
	defer func() {
		if r := recover(); r != nil {
			logp.Err("Panic %v: json: %s, base64: %s", r, string(data), buffer.String())
		}
	}()

	events := []map[string]interface{}{}
	for _, o := range obj {
		// if o.Content.Private != nil { // exclude private event
		// 	logp.Info("this is a private event.\nEvent: %+v", marshalMap(o.Content))
		// 	continue
		// }

		m, err := transformMap(o)
		if err != nil {
			logp.Err("Fail to transform map with err: %s;\nEvent.Content: %s", err, marshalMap(o.Content))
			continue
		}

		if m == nil { // ignore
			continue
		}

		events = append(events, m)
	}

	data, err = json.Marshal(events)
	if err != nil {
		return nil, errors.Wrap(err, "fail to json marshal events")
	}

	encoder := base64.NewEncoder(base64.StdEncoding, buffer)
	if _, err := encoder.Write(data); err != nil {
		return nil, errors.Wrap(err, "fail to base64 encode data")
	}
	encoder.Close()

	if _, err := ge.writer.Write(buffer.Bytes()); err != nil {
		return nil, errors.Wrap(err, "fail to gzip write data")
	}
	if err := ge.writer.Flush(); err != nil {
		return nil, errors.Wrap(err, "fail to gzip flush")
	}
	if err := ge.writer.Close(); err != nil {
		return nil, errors.Wrap(err, "fail to gzip close")
	}

	b := bytes.NewBuffer(nil)
	if _, err := io.Copy(b, ge.buf); err != nil {
		return nil, errors.Wrap(err, "fail to copy buf")
	}
	return b, nil
}

func transformMap(event publisher.Event) (map[string]interface{}, error) {
	source, err := event.Content.GetValue("terminus.source")
	if err != nil {
		source = "container"
	}
	id, err := event.Content.GetValue("terminus.id")
	if err != nil {
		return nil, errors.Wrap(err, "fail to get id value")
	}
	offset, err := event.Content.GetValue("log.offset")
	if err != nil {
		return nil, errors.Wrap(err, "fail to get offset value")
	}
	stream, err := event.Content.GetValue("stream")
	if err != nil {
		stream = "stdout"
	}
	message, err := event.Content.GetValue("message")
	if err != nil {
		return nil, errors.Wrap(err, "fail to get message value")
	}
	tags := make(map[string]string)
	if d, err := event.Content.GetValue("terminus.tags"); err == nil {
		if v, err := convert(d); err == nil {
			tags = v
		}
	}
	labels := make(map[string]string)
	if d, err := event.Content.GetValue("terminus.labels"); err == nil {
		if v, err := convert(d); err == nil {
			labels = v
		}
	}
	m := make(map[string]interface{})
	m["source"] = source
	m["id"] = id
	m["offset"] = offset
	m["timestamp"] = event.Content.Timestamp.UnixNano()
	m["stream"] = stream
	m["content"] = message
	m["tags"] = tags
	m["labels"] = labels
	logp.Debug(selector, "transformMap get final message: %+v", marshalMap(m))
	return m, nil
}

func marshalMap(m interface{}) string {
	d, _ := json.Marshal(m)
	return string(d)
}

func convert(data interface{}) (map[string]string, error) {
	m, err := convertToMap(data)
	if err != nil {
		return nil, err
	}
	m = handle(m)
	return m, nil
}

func convertToMap(data interface{}) (map[string]string, error) {
	res := make(map[string]string)
	switch data.(type) {
	case map[string]string:
		res = data.(map[string]string)
	case common.MapStr:
		tmp := data.(common.MapStr)
		for k, v := range tmp {
			switch val := v.(type) {
			case string:
				res[k] = val
			case uint64:
				res[k] = strconv.Itoa(int(val))
			case uint32:
				res[k] = strconv.Itoa(int(val))
			case int64:
				res[k] = strconv.Itoa(int(val))
			case int32:
				res[k] = strconv.Itoa(int(val))
			case float64:
				res[k] = strconv.Itoa(int(val))
			case float32:
				res[k] = strconv.Itoa(int(val))
			}
			if v, ok := v.(string); ok {
				res[k] = v
			}
		}
	default:
		return nil, errors.New(fmt.Sprintf("no supported type: %s", reflect.TypeOf(data).Name()))
	}
	return res, nil
}

func handle(m map[string]string) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		if reflect.DeepEqual(v, reflect.Zero(reflect.TypeOf(v)).Interface()) {
			continue
		}
		res[strings.ToLower(k)] = v
	}
	return res
}
