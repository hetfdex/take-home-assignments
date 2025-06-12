package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

type logsProcessor struct {
	attributeKey       string
	processingInterval time.Duration
	attributeCounter   map[string]int
	numberOfWorkers    int

	attributeChan chan string
	shutdownChan  chan struct{}
	mu            sync.RWMutex
	wg            sync.WaitGroup
}

func newLogsProcessor(
	attributeKey string,
	processingInterval time.Duration,
	bufferSize int,
	numberOfWorkers int,
) (*logsProcessor, error) {
	if attributeKey == "" {
		return nil, fmt.Errorf("attributeKey cannot be empty")
	}

	if processingInterval <= 0 {
		return nil, fmt.Errorf("processingInterval must be positive")
	}

	if bufferSize <= 0 {
		return nil, fmt.Errorf("bufferSize must be positive")
	}

	if numberOfWorkers <= 0 {
		return nil, fmt.Errorf("numberOfWorkers must be positive")
	}

	logsProcessor := &logsProcessor{
		attributeKey:       attributeKey,
		processingInterval: processingInterval,
		numberOfWorkers:    numberOfWorkers,
		attributeCounter:   make(map[string]int),
		attributeChan:      make(chan string, bufferSize),
		shutdownChan:       make(chan struct{}),
	}

	logsProcessor.startWorkers()

	go logsProcessor.startProcessor()

	return logsProcessor, nil
}

func (p *logsProcessor) startWorkers() {
	for range p.numberOfWorkers {
		p.wg.Add(1)

		go func() {
			defer p.wg.Done()

			for {
				select {
				case attribute := <-p.attributeChan:
					p.addAttributeToCounter(attribute)
				case <-p.shutdownChan:

					return
				}
			}
		}()
	}
}

func (p *logsProcessor) addAttributeToCounter(attribute string) {
	p.mu.Lock()

	defer p.mu.Unlock()

	p.attributeCounter[attribute]++
}

func (p *logsProcessor) startProcessor() {
	ticker := time.NewTicker(p.processingInterval)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.restartProcessor()
		case <-p.shutdownChan:
			return
		}
	}
}

func (p *logsProcessor) restartProcessor() {
	p.mu.Lock()

	defer p.mu.Unlock()

	if len(p.attributeCounter) > 0 {
		p.writeReport()
	}

	p.attributeCounter = make(map[string]int)
}

func (p *logsProcessor) writeReport() {
	fmt.Println("=== Log Counts Report ===")

	if len(p.attributeCounter) == 0 {
		fmt.Println("  No logs processed in this interval")
	} else {
		for attributeValue, attributeCount := range p.attributeCounter {
			fmt.Printf("  %s: %d unique logs\n", attributeValue, attributeCount)
		}
	}

	fmt.Println("=====================================")
}

func (p *logsProcessor) processLogs(
	ctx context.Context,
	request *collogspb.ExportLogsServiceRequest) error {
	for _, resourceLog := range request.GetResourceLogs() {
		resourceLogsMap := attributesToMap(resourceLog.GetResource().GetAttributes())

		for _, scopeLog := range resourceLog.GetScopeLogs() {
			scopeLogsMap := attributesToMap(scopeLog.GetScope().GetAttributes())

			for _, logRecord := range scopeLog.GetLogRecords() {
				logRecordsMap := attributesToMap(logRecord.GetAttributes())

				err := p.processLog(
					ctx,
					resourceLogsMap,
					scopeLogsMap,
					logRecordsMap,
				)

				if err != nil {
					return fmt.Errorf("failed to process log record: %w", err)
				}
			}
		}
	}

	return nil
}

func (p *logsProcessor) processLog(
	ctx context.Context,
	resourceLogsMap map[string]any,
	scopeLogsMap map[string]any,
	logRecordsMap map[string]any,
) error {
	if attribute, exists := getStringAttribute(resourceLogsMap, p.attributeKey); exists {
		p.queueLog(ctx, attribute)
	}

	if attribute, exists := getStringAttribute(scopeLogsMap, p.attributeKey); exists {
		p.queueLog(ctx, attribute)
	}

	if attribute, exists := getStringAttribute(logRecordsMap, p.attributeKey); exists {
		p.queueLog(ctx, attribute)
	}

	if !hasAttributeAnywhere(resourceLogsMap, scopeLogsMap, logRecordsMap, p.attributeKey) {
		p.queueLog(ctx, "unknown")
	}

	return nil
}

func (p *logsProcessor) queueLog(ctx context.Context, attributeValue string) {
	select {
	case p.attributeChan <- attributeValue:
		// log queued
	default:
		// exceeded buffer size
		slog.ErrorContext(ctx, "Queue is full, dropping log", slog.String("attribute", attributeValue))
	}
}

func (p *logsProcessor) shutdown(ctx context.Context) error {
	close(p.shutdownChan)

	done := make(chan struct{})

	go func() {
		p.wg.Wait()

		close(done)
	}()

	select {
	case <-done:
		// all good
		return nil
	case <-ctx.Done():
		slog.ErrorContext(ctx, "Shutdown timeout exceeded")

		return ctx.Err()
	}
}

func attributesToMap(attributes []*commonpb.KeyValue) map[string]any {
	res := make(map[string]any, len(attributes))

	for _, attribute := range attributes {
		if attribute.Key != "" {
			res[attribute.Key] = anyValueToString(attribute.Value)
		}
	}

	return res
}

func anyValueToString(value *commonpb.AnyValue) string {
	if value == nil {
		return ""
	}

	switch v := value.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(v.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(v.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return string(v.BytesValue)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func getStringAttribute(attributes map[string]any, key string) (string, bool) {
	if attributes == nil {
		return "", false
	}

	val, exists := attributes[key]

	if !exists {
		return "", false
	}

	str, ok := val.(string)

	if !ok {
		return fmt.Sprintf("%v", val), true
	}

	return str, true
}

func hasAttributeAnywhere(
	resourceLogsMap map[string]any,
	scopeLogsMap map[string]any,
	logRecordsMap map[string]any,
	attributeKey string,
) bool {
	_, resourceExists := getStringAttribute(resourceLogsMap, attributeKey)
	_, scopeExists := getStringAttribute(scopeLogsMap, attributeKey)
	_, logExists := getStringAttribute(logRecordsMap, attributeKey)

	return resourceExists || scopeExists || logExists
}
