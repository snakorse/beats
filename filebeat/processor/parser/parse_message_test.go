package parser

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseMessage_parseV2(t *testing.T) {
	pro, err := newParseMessage(nil)
	assert.Nil(t, err)
	p := pro.(*parseMessage)

	type args struct {
		message string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 map[string]interface{}
	}{
		{
			"1",
			args{message: "2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b,userid=1,orderid=xyz] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start"},
			"2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start",
			map[string]interface{}{
				"request-id": "07409b69-2595-4e38-b895-5846cf1e0d8b",
				"userid":     "1",
				"orderid":    "xyz",
				"level":      "INFO",
			},
		},
		{
			"2",
			args{message: "2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start"},
			"2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start",
			map[string]interface{}{
				"request-id": "07409b69-2595-4e38-b895-5846cf1e0d8b",
				"level":      "INFO",
			},
		},
		{
			"2.5",
			args{message: "2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b,07409b69-2595-4e38-b895-5846cf1e0das] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start"},
			"2018-11-22 11:02:35.541 INFO [pmp,07409b69-2595-4e38-b895-5846cf1e0d8b,07409b69-2595-4e38-b895-5846cf1e0das] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start",
			map[string]interface{}{
				"request-id": "07409b69-2595-4e38-b895-5846cf1e0d8b",
				"level":      "INFO",
			},
		},
		{
			"3",
			args{message: "2018-11-22 11:02:35.541 INFO [07409b69-2595-4e38-b895-5846cf1e0d8b] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start"},
			"2018-11-22 11:02:35.541 INFO [07409b69-2595-4e38-b895-5846cf1e0d8b] - [main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start",
			map[string]interface{}{
				"level":      "INFO",
			},
		},
		{
			"4",
			args{message: "2018-11-22 11:02:35.541 INFO xxx - xxx o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start"},
			"2018-11-22 11:02:35.541 INFO xxx - xxx o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on start",
			map[string]interface{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := p.parseV2(tt.args.message)
			if got != tt.want {
				t.Errorf("parseV2() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("parseV2() got1 = %v, want %v", got1, tt.want1)
			}

		})
	}
}
