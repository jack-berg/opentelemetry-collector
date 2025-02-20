// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build enable_unstable
// +build enable_unstable

package internal

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	"go.opentelemetry.io/collector/model/pdata"
)

func createTestQueue(extension storage.Extension, capacity int) *persistentQueue {
	logger, _ := zap.NewDevelopment()

	client, err := extension.GetClient(context.Background(), component.KindReceiver, config.ComponentID{}, "")
	if err != nil {
		panic(err)
	}

	wq := NewPersistentQueue(context.Background(), "foo", capacity, logger, client, newFakeTracesRequestUnmarshalerFunc())
	return wq.(*persistentQueue)
}

func TestPersistentQueue_Capacity(t *testing.T) {
	path := createTemporaryDirectory()
	defer os.RemoveAll(path)

	for i := 0; i < 100; i++ {
		ext := createStorageExtension(path)
		t.Cleanup(func() { require.NoError(t, ext.Shutdown(context.Background())) })

		wq := createTestQueue(ext, 5)
		require.Equal(t, 0, wq.Size())

		traces := newTraces(1, 10)
		req := newFakeTracesRequest(traces)

		for i := 0; i < 10; i++ {
			result := wq.Produce(req)
			if i < 6 {
				require.True(t, result)
			} else {
				require.False(t, result)
			}

			// Let's make sure the loop picks the first element into the channel,
			// so the capacity could be used in full
			if i == 0 {
				require.Eventually(t, func() bool {
					return wq.Size() == 0
				}, 1000*time.Millisecond, 10*time.Millisecond)
			}
		}
		require.Equal(t, 5, wq.Size())
	}
}

func TestPersistentQueue_Close(t *testing.T) {
	path := createTemporaryDirectory()
	defer os.RemoveAll(path)

	ext := createStorageExtension(path)
	t.Cleanup(func() { require.NoError(t, ext.Shutdown(context.Background())) })

	wq := createTestQueue(ext, 1001)
	traces := newTraces(1, 10)
	req := newFakeTracesRequest(traces)

	wq.StartConsumers(100, func(item interface{}) {})

	for i := 0; i < 1000; i++ {
		wq.Produce(req)
	}
	// This will close the queue very quickly, consumers might not be able to consume anything and should finish gracefully
	require.Eventually(t, func() bool {
		wq.Stop()
		return true
	}, 500*time.Millisecond, 10*time.Millisecond)
	// The additional stop should not panic
	require.Eventually(t, func() bool {
		wq.Stop()
		return true
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestPersistentQueue_ConsumersProducers(t *testing.T) {
	cases := []struct {
		numMessagesProduced int
		numConsumers        int
	}{
		{
			numMessagesProduced: 1,
			numConsumers:        1,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        1,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        3,
		},
		{
			numMessagesProduced: 1,
			numConsumers:        100,
		},
		{
			numMessagesProduced: 100,
			numConsumers:        100,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("#messages: %d #consumers: %d", c.numMessagesProduced, c.numConsumers), func(t *testing.T) {
			path := createTemporaryDirectory()

			traces := newTraces(1, 10)
			req := newFakeTracesRequest(traces)

			ext := createStorageExtension(path)
			tq := createTestQueue(ext, 5000)

			defer os.RemoveAll(path)
			defer tq.Stop()
			t.Cleanup(func() { require.NoError(t, ext.Shutdown(context.Background())) })

			numMessagesConsumed := int32(0)
			tq.StartConsumers(c.numConsumers, func(item interface{}) {
				if item != nil {
					atomic.AddInt32(&numMessagesConsumed, 1)
				}
			})

			for i := 0; i < c.numMessagesProduced; i++ {
				tq.Produce(req)
			}

			require.Eventually(t, func() bool {
				return c.numMessagesProduced == int(atomic.LoadInt32(&numMessagesConsumed))
			}, 500*time.Millisecond, 10*time.Millisecond)
		})
	}
}

func newTraces(numTraces int, numSpans int) pdata.Traces {
	traces := pdata.NewTraces()
	batch := traces.ResourceSpans().AppendEmpty()
	batch.Resource().Attributes().InsertString("resource-attr", "some-resource")
	batch.Resource().Attributes().InsertInt("num-traces", int64(numTraces))
	batch.Resource().Attributes().InsertInt("num-spans", int64(numSpans))

	for i := 0; i < numTraces; i++ {
		traceID := pdata.NewTraceID([16]byte{1, 2, 3, byte(i)})
		ils := batch.InstrumentationLibrarySpans().AppendEmpty()
		for j := 0; j < numSpans; j++ {
			span := ils.Spans().AppendEmpty()
			span.SetTraceID(traceID)
			span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, byte(j)}))
			span.SetName("should-not-be-changed")
			span.Attributes().InsertInt("int-attribute", int64(j))
			span.Attributes().InsertString("str-attribute-1", "foobar")
			span.Attributes().InsertString("str-attribute-2", "fdslafjasdk12312312jkl")
			span.Attributes().InsertString("str-attribute-3", "AbcDefGeKKjkfdsafasdfsdasdf")
			span.Attributes().InsertString("str-attribute-4", "xxxxxx")
			span.Attributes().InsertString("str-attribute-5", "abcdef")
		}
	}

	return traces
}
