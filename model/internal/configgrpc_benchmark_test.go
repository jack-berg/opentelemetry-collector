/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package internal

import (
	"bytes"
	"fmt"
	"github.com/mostynb/go-grpc-compression/snappy"
	"github.com/mostynb/go-grpc-compression/zstd"
	collector_internal_data "go.opentelemetry.io/collector/model/internal/data"
	otlpcollectorlog "go.opentelemetry.io/collector/model/internal/data/protogen/collector/logs/v1"
	otlpcollectormetrics "go.opentelemetry.io/collector/model/internal/data/protogen/collector/metrics/v1"
	otlpcollectortrace "go.opentelemetry.io/collector/model/internal/data/protogen/collector/trace/v1"
	otlpcommon "go.opentelemetry.io/collector/model/internal/data/protogen/common/v1"
	otlplog "go.opentelemetry.io/collector/model/internal/data/protogen/logs/v1"
	otlpmetrics "go.opentelemetry.io/collector/model/internal/data/protogen/metrics/v1"
	otlpresource "go.opentelemetry.io/collector/model/internal/data/protogen/resource/v1"
	otlptrace "go.opentelemetry.io/collector/model/internal/data/protogen/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/encoding"
)

type testPayload struct {
	name    string
	message proto.Message
}

func BenchmarkCompressors(b *testing.B) {
	payloads := setupTestPayloads()

	compressors := make([]encoding.Compressor, 0)
	compressors = append(compressors, encoding.GetCompressor(gzip.Name))
	compressors = append(compressors, encoding.GetCompressor(zstd.Name))
	compressors = append(compressors, encoding.GetCompressor(snappy.Name))

	codec := encoding.GetCodec("proto")

	for _, payload := range payloads {
		for _, compressor := range compressors {
			messageBytes, err := codec.Marshal(payload.message)
			if err != nil {
				b.Errorf("codec.Marshal(_) returned an error")
			}

			compressedBytes, err := compress(compressor, messageBytes)
			if err != nil {
				b.Errorf("Compressor.Compress(_) returned an error")
			}

			name := fmt.Sprintf("%v/raw_bytes_%v/compressed_bytes_%v/compressor_%v", payload.name, len(messageBytes), len(compressedBytes), compressor.Name())

			b.Run(name, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					messageBytes, err := codec.Marshal(payload.message)
					if err != nil {
						b.Errorf("codec.Marshal(_) returned an error")
					}
					compress(compressor, messageBytes)
				}
			})
		}
	}
}

func compress(compressor encoding.Compressor, in []byte) ([]byte, error) {
	if compressor == nil {
		return nil, nil
	}
	wrapErr := func(err error) error {
		return status.Errorf(codes.Internal, "error while compressing: %v", err.Error())
	}
	cbuf := &bytes.Buffer{}
	z, err := compressor.Compress(cbuf)
	if err != nil {
		return nil, wrapErr(err)
	}
	if _, err := z.Write(in); err != nil {
		return nil, wrapErr(err)
	}
	if err := z.Close(); err != nil {
		return nil, wrapErr(err)
	}
	return cbuf.Bytes(), nil
}

func setupTestPayloads() []testPayload {
	payloads := make([]testPayload, 0)

	payloads = append(payloads, testPayload{
		name: "sm_log_request",
		message: &otlpcollectorlog.ExportLogsServiceRequest{ResourceLogs: []*otlplog.ResourceLogs{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibraryLogs: []*otlplog.InstrumentationLibraryLogs{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Logs: []*otlplog.LogRecord{
							logRecord("log message one"),
						},
					},
				},
			},
		}}})

	logs := make([]*otlplog.LogRecord, 0)
	for i := 0; i < 500; i++ {
		logs = append(logs, logRecord("log message "+RandStringRunes(50)))
	}
	payloads = append(payloads, testPayload{
		name: "lg_log_request",
		message: &otlpcollectorlog.ExportLogsServiceRequest{ResourceLogs: []*otlplog.ResourceLogs{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibraryLogs: []*otlplog.InstrumentationLibraryLogs{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Logs:                   logs,
					},
				},
			},
		}}})

	payloads = append(payloads, testPayload{
		name: "sm_trace_request",
		message: &otlpcollectortrace.ExportTraceServiceRequest{ResourceSpans: []*otlptrace.ResourceSpans{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Spans: []*otlptrace.Span{
							span("GET /foo"),
						},
					},
				},
			},
		}}})

	spans := make([]*otlptrace.Span, 0)
	for i := 0; i < 500; i++ {
		spans = append(spans, span("GET "+RandStringRunes(10)))
	}
	payloads = append(payloads, testPayload{
		name: "lg_trace_request",
		message: &otlpcollectortrace.ExportTraceServiceRequest{ResourceSpans: []*otlptrace.ResourceSpans{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Spans:                  spans,
					},
				},
			},
		}}})

	payloads = append(payloads, testPayload{
		name: "sm_metric_request",
		message: &otlpcollectormetrics.ExportMetricsServiceRequest{ResourceMetrics: []*otlpmetrics.ResourceMetrics{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Metrics: []*otlpmetrics.Metric{
							metric("metric-name"),
						},
					},
				},
			},
		}}})

	metrics := make([]*otlpmetrics.Metric, 0)
	for i := 0; i < 500; i++ {
		metrics = append(metrics, metric("metric-"+RandStringRunes(10)))
	}
	payloads = append(payloads, testPayload{
		name: "lg_metric_request",
		message: &otlpcollectormetrics.ExportMetricsServiceRequest{ResourceMetrics: []*otlpmetrics.ResourceMetrics{
			{
				Resource: resource("my-service", "instance-id-1234"),
				InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
					{
						InstrumentationLibrary: instrumentationLibrary("my-library", "1.0.0"),
						Metrics:                metrics,
					},
				},
			},
		}}})

	return payloads
}

func resource(serviceName string, instanceId string) otlpresource.Resource {
	return otlpresource.Resource{
		Attributes: []otlpcommon.KeyValue{
			{
				Key:   "service.name",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: serviceName}},
			},
			{
				Key:   "service.instance.id",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: instanceId}},
			},
		},
	}
}

func instrumentationLibrary(name string, version string) otlpcommon.InstrumentationLibrary {
	return otlpcommon.InstrumentationLibrary{
		Name:    name,
		Version: version,
	}
}

func logRecord(message string) *otlplog.LogRecord {
	severityNumber := otlplog.SeverityNumber(rand.Intn(24))

	return &otlplog.LogRecord{
		TimeUnixNano:   uint64(time.Now().UnixNano()),
		SeverityNumber: severityNumber,
		SeverityText:   severityNumber.String(),
		Body:           otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: message}},
		Attributes: []otlpcommon.KeyValue{
			{
				Key:   "key1",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value1"}},
			},
			{
				Key:   "key2",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value2"}},
			},
		},
		DroppedAttributesCount: 1,
		Flags:                  0,
	}
}

func span(name string) *otlptrace.Span {
	spanKind := otlptrace.Span_SpanKind(rand.Intn(5))
	return &otlptrace.Span{
		TraceId:           collector_internal_data.NewTraceID([16]byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78}),
		SpanId:            collector_internal_data.NewSpanID([8]byte{0x12, 0x23, 0xAD, 0x12, 0x23, 0xAD, 0x12, 0x23}),
		TraceState:        "",
		Name:              name,
		Kind:              spanKind,
		StartTimeUnixNano: uint64(time.Now().UnixNano()) - 1e9,
		EndTimeUnixNano:   uint64(time.Now().UnixNano()),
		Attributes: []otlpcommon.KeyValue{
			{
				Key:   "key1",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value1"}},
			},
			{
				Key:   "key2",
				Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value2"}},
			},
		},
		DroppedAttributesCount: 0,
		Events:                 nil,
		DroppedEventsCount:     0,
		Links:                  nil,
		DroppedLinksCount:      0,
		Status:                 otlptrace.Status{},
	}
}

func metric(name string) *otlpmetrics.Metric {
	return &otlpmetrics.Metric{
		Name:        name,
		Description: "metric description",
		Unit:        "",
		Data: &otlpmetrics.Metric_Gauge{
			Gauge: &otlpmetrics.Gauge{
				DataPoints: []*otlpmetrics.NumberDataPoint{
					{
						StartTimeUnixNano: uint64(time.Now().UnixNano()) - 60e9,
						TimeUnixNano:      uint64(time.Now().UnixNano()),
						Value:             &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: rand.Float64()},
						Attributes: []otlpcommon.KeyValue{
							{
								Key:   "key1",
								Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value1"}},
							},
							{
								Key:   "key2",
								Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value2"}},
							},
						},
					},
					{
						StartTimeUnixNano: uint64(time.Now().UnixNano()) - 60e9,
						TimeUnixNano:      uint64(time.Now().UnixNano()),
						Value:             &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: rand.Float64()},
						Attributes: []otlpcommon.KeyValue{
							{
								Key:   "key1",
								Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value3"}},
							},
							{
								Key:   "key2",
								Value: otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "value4"}},
							},
						},
					},
				},
			},
		},
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
