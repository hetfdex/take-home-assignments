package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

type logsProcessor struct {
	logger *slog.Logger // the logger to use for logging messages
	writer io.Writer    // the writer to output the log counts report

	attributeKey       string         // the key of the attribute to count logs by
	processingInterval time.Duration  // the interval at which logs are processed
	attributeCounter   map[string]int // a map to count occurrences of each attribute value
	numberOfWorkers    int            // the number of workers to process logs concurrently

	attributeChan chan string    // a channel to receive attributes for processing
	shutdownChan  chan struct{}  // a channel to signal shutdown of the processor
	mu            sync.RWMutex   // a mutex to protect access to the attributeCounter map
	wg            sync.WaitGroup // a wait group to wait for all workers to finish
}

func newLogsProcessor(
	logger *slog.Logger,
	writer io.Writer,
	attributeKey string,
	processingInterval time.Duration,
	attributeChanSize int,
	numberOfWorkers int,
) (*logsProcessor, error) {
	if attributeKey == "" {
		return nil, fmt.Errorf("attributeKey cannot be empty")
	}

	if processingInterval <= 0 {
		return nil, fmt.Errorf("processingInterval must be positive")
	}

	if attributeChanSize <= 0 {
		return nil, fmt.Errorf("attributeChanSize must be positive")
	}

	if numberOfWorkers <= 0 {
		return nil, fmt.Errorf("numberOfWorkers must be positive")
	}

	logsProcessor := &logsProcessor{
		logger:             logger,
		writer:             writer,
		attributeKey:       attributeKey,
		processingInterval: processingInterval,
		numberOfWorkers:    numberOfWorkers,
		attributeCounter:   make(map[string]int),
		attributeChan:      make(chan string, attributeChanSize),
		shutdownChan:       make(chan struct{}),
	}

	logsProcessor.startWorkers()

	go logsProcessor.startProcessor()

	return logsProcessor, nil
}

// startWorkers starts the specified number of worker goroutines that will
// listen for attributes on the attributeChan and add them to the attributeCounter.
// It uses a wait group to ensure that all workers are properly shut down when
// the shutdownChan is closed.
// Each worker will process attributes concurrently, allowing for efficient counting
// of logs based on the specified attribute key.
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

// addAttributeToCounter increments the count for the given attribute in the
// attributeCounter map. It uses a mutex to ensure that access to the map is
// thread-safe, allowing multiple workers to safely update the count concurrently.
func (p *logsProcessor) addAttributeToCounter(attribute string) {
	p.mu.Lock()

	defer p.mu.Unlock()

	p.attributeCounter[attribute]++
}

// startProcessor starts a ticker that triggers the processing of logs at the
// specified processingInterval. It restarts the processor when the ticker ticks.
// When restarted, it writes the current log counts report to the writer and resets
// the attributeCounter map. This allows for periodic reporting of log counts while
// ensuring that the processor can be gracefully shut down when needed.
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

// restartProcessor writes the current log counts report to the writer and resets
// the attributeCounter map.
func (p *logsProcessor) restartProcessor() {
	p.mu.Lock()

	defer p.mu.Unlock()

	if len(p.attributeCounter) > 0 {
		p.writeReport()
	}

	p.attributeCounter = make(map[string]int)
}

// writeReport writes the current log counts report to the writer.
func (p *logsProcessor) writeReport() {
	fmt.Fprintln(p.writer, "=== Log Counts Report ===")

	for attributeValue, attributeCount := range p.attributeCounter {
		fmt.Fprintf(p.writer, "%s - %d\n", attributeValue, attributeCount)
	}

	fmt.Fprintln(p.writer, "=====================================")
}

// processLogs processes the logs from the ExportLogsServiceRequest.
// It iterates through the resource logs, scope logs, and log records, extracting
// the specified attribute and queuing it for processing. If the attribute is not
// found in any of the logs, it queues a default "unknown" value.
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

// processLog processes a single log record by checking for the specified attribute
// in the resource logs, scope logs, and log records. If the attribute is found,
// it queues the attribute value for processing. If the attribute is not found in
// any of the logs, it queues a default "unknown" value. This allows for efficient
// counting of logs based on the specified attribute key, while also handling cases
// where the attribute may not be present in all logs.
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

// queueLog queues the given attribute value for processing. If the channel is full,
// it logs an error message and drops the log. This ensures that the processor can
// handle high volumes of logs without blocking, while also providing feedback
// when logs are dropped due to a full queue.
func (p *logsProcessor) queueLog(ctx context.Context, attributeValue string) {
	select {
	case p.attributeChan <- attributeValue:
		// log is queued for processing
	default:
		// log is dropped because the channel is full
		p.logger.ErrorContext(ctx, "Queue is full, dropping log", slog.String("attribute", attributeValue))
	}
}

// shutdown gracefully shuts down the logs processor by closing the shutdown channel
// and waiting for all worker goroutines to finish. It uses a context with a timeout
// to ensure that the shutdown process does not hang indefinitely. This allows for
// a clean shutdown of the processor, ensuring that all resources are released and
// no logs are left unprocessed.
func (p *logsProcessor) shutdown(ctx context.Context) error {
	close(p.shutdownChan)

	done := make(chan struct{})

	go func() {
		p.wg.Wait()

		close(done)
	}()

	select {
	case <-done:
		// all workers have been shut down
		return nil
	case <-ctx.Done():
		p.logger.ErrorContext(ctx, "Shutdown timeout exceeded")

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

// anyValueToString converts an AnyValue to a string representation.
// It handles different types of values such as string, int, double, bool, and bytes.
// If the value is nil or of an unsupported type, it returns a default string representation.
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
