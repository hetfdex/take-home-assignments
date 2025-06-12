package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestLogProcessor(t *testing.T) {
	t.Run("processes logs correctly with string values", func(t *testing.T) {
		processor := makeLogsProcessor(t, nil, &safeWriter{}, 100, 5)

		defer processor.shutdown(context.Background())

		err := processor.processLogs(context.Background(), makeStringValuesReq())

		assert.NoError(t, err)

		waitForProcessing(t, processor)

		processor.mu.RLock()

		defer processor.mu.RUnlock()

		assert.Equal(t, 2, processor.attributeCounter["some_resource_value"])
		assert.Equal(t, 1, processor.attributeCounter["some_scope_value"])
		assert.Equal(t, 1, processor.attributeCounter["some_log_value"])
		assert.Equal(t, 1, processor.attributeCounter["unknown"])
	})

	t.Run("processes logs correctly with other values", func(t *testing.T) {
		processor := makeLogsProcessor(t, nil, &safeWriter{}, 100, 5)

		defer processor.shutdown(context.Background())

		err := processor.processLogs(context.Background(), makeOtherValuesReq())

		assert.NoError(t, err)

		waitForProcessing(t, processor)

		processor.mu.RLock()

		defer processor.mu.RUnlock()

		assert.Equal(t, 1, processor.attributeCounter["true"])
		assert.Equal(t, 1, processor.attributeCounter["1"])
		assert.Equal(t, 1, processor.attributeCounter["2"])
		assert.Equal(t, 1, processor.attributeCounter["bytes"])
		assert.Equal(t, 0, processor.attributeCounter["unknown"])

	})

	t.Run("outputs report correctly", func(t *testing.T) {
		writer := &safeWriter{}

		processor := makeLogsProcessor(t, nil, writer, 100, 5)

		defer processor.shutdown(context.Background())

		err := processor.processLogs(context.Background(), makeStringValuesReq())

		assert.NoError(t, err)

		waitForProcessing(t, processor)

		// wait for report
		time.Sleep(200 * time.Millisecond)

		outputStr := writer.String()

		assert.Contains(t, outputStr, "some_resource_value - 2")
		assert.Contains(t, outputStr, "some_scope_value - 1")
		assert.Contains(t, outputStr, "some_log_value - 1")
		assert.Contains(t, outputStr, "unknown - 1")
	})

	t.Run("restarts correctly", func(t *testing.T) {
		processor := makeLogsProcessor(t, nil, &safeWriter{}, 100, 5)

		defer processor.shutdown(context.Background())

		err := processor.processLogs(context.Background(), makeOtherValuesReq())

		assert.NoError(t, err)

		waitForProcessing(t, processor)

		processor.mu.RLock()

		assert.Equal(t, 5, len(processor.attributeCounter))

		processor.mu.RUnlock()

		// wait for restart
		time.Sleep(200 * time.Millisecond)

		processor.mu.RLock()

		assert.Empty(t, processor.attributeCounter)

		processor.mu.RUnlock()
	})

	t.Run("drops logs when queue is full", func(t *testing.T) {
		var logsBuffer strings.Builder

		logger := slog.New(
			slog.NewTextHandler(
				&logsBuffer,
				&slog.HandlerOptions{
					Level: slog.LevelError,
				},
			),
		)

		// small queue size and single worker to trigger the full condition
		processor := makeLogsProcessor(t, logger, &safeWriter{}, 2, 1)

		defer processor.shutdown(context.Background())

		// fill the queue to trigger the full condition
		for i := range 20 {
			processor.queueLog(context.Background(), fmt.Sprintf("log_%d", i))
		}

		output := logsBuffer.String()

		assert.Contains(t, output, "Queue is full, dropping log")
	})
}

func makeLogsProcessor(t *testing.T,
	logger *slog.Logger,
	safeWriter *safeWriter,
	attributeChanSize int,
	numberOfWorkers int,
) *logsProcessor {
	t.Helper()

	processor, err := newLogsProcessor(
		logger,
		safeWriter,
		"foo",
		100*time.Millisecond,
		attributeChanSize,
		numberOfWorkers,
	)

	assert.NoError(t, err)

	return processor
}

// waitForProcessing waits for the logs processor to finish processing logs.
// It checks the attribute channel and the attribute counter map to ensure
// that the processing has stabilized, meaning no new attributes are being added
// and the size of the attribute counter map remains constant for a certain number
// of iterations. If the processing does not stabilize within the timeout period,
// it fails the test. This function is useful for ensuring that the logs processor
// has completed its work before proceeding with assertions in tests.
func waitForProcessing(
	t *testing.T,
	processor *logsProcessor,
) {
	t.Helper()

	timeout := time.After(30 * time.Second)

	ticker := time.NewTicker(10 * time.Millisecond)

	defer ticker.Stop()

	var lastCounterSize int

	stableCount := 0

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for processing to complete")
		case <-ticker.C:
			if len(processor.attributeChan) == 0 {
				processor.mu.RLock()

				currentSize := len(processor.attributeCounter)

				processor.mu.RUnlock()

				if currentSize == lastCounterSize {
					stableCount++

					if stableCount >= 3 {
						return
					}
				} else {
					stableCount = 0

					lastCounterSize = currentSize
				}
			} else {
				stableCount = 0
			}
		}
	}
}

// safeWriter is a thread-safe writer that uses a mutex to ensure that writes
// to the underlying strings.Builder are synchronized. This is useful in a
// concurrent environment where multiple goroutines may attempt to write to the
// same writer at the same time.
type safeWriter struct {
	mu      sync.RWMutex
	builder strings.Builder
}

// Write implements the io.Writer interface for safeWriter. It locks the mutex
// before writing to the underlying strings.Builder to ensure that writes are
// synchronized across multiple goroutines.
func (sw *safeWriter) Write(p []byte) (n int, err error) {
	sw.mu.Lock()

	defer sw.mu.Unlock()

	return sw.builder.Write(p)
}

// String returns the string representation of the underlying strings.Builder.
// It locks the mutex for reading to ensure that the string is read safely
// in a concurrent environment.
func (sw *safeWriter) String() string {
	sw.mu.RLock()

	defer sw.mu.RUnlock()

	return sw.builder.String()
}

func makeStringValuesReq() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "foo",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "some_resource_value",
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: []*commonpb.KeyValue{
								{
									Key: "foo",
									Value: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{
											StringValue: "some_scope_value",
										},
									},
								},
							},
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "foo",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "some_log_value",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "foo",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "some_resource_value",
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: []*commonpb.KeyValue{
								{
									Key: "bar",
									Value: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{
											StringValue: "some_scope_value",
										},
									},
								},
							},
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "bar",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "some_log_value",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "bar",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "some_resource_value",
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: []*commonpb.KeyValue{
								{
									Key: "bar",
									Value: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{
											StringValue: "some_scope_value",
										},
									},
								},
							},
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "bar",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "some_log_value",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeOtherValuesReq() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "foo",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_IntValue{
									IntValue: 1,
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: []*commonpb.KeyValue{
								{
									Key: "foo",
									Value: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_DoubleValue{
											DoubleValue: 2.0,
										},
									},
								},
							},
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "foo",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_BoolValue{
												BoolValue: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "foo",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_BytesValue{
									BytesValue: []byte("bytes"),
								},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: []*commonpb.KeyValue{
								{
									Key: "foo",
									Value: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{
											StringValue: "some_value",
										},
									},
								},
							},
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "foo",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "some_value",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
