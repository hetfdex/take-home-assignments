# How the Log Processor Works

The heart of the service is the `logs_processor` file. I went with a concurrent approach that handles log data with high throughput in mind.

## Architecture Overview
The processor uses a worker pool pattern with three main components:

Incoming logs get converted to attribute values and queued in a buffered channel. This decouples the fast gRPC receive path from the slower processing work.

Multiple configurable goroutines continuously pull attribute values from the queue and update thread-safe counters. Each worker increments counts for specific attribute values it processes.

A background ticker runs every configurable interval to dump the current counts to stdout, then resets all counters for the next window.

## Processing Flow
When log data arrives, the processor extracts attributes from the three levels of the OpenTelemetry data structure and looks for your configured attribute key at each level.

It then converts any found values to strings and if the attribute isn't found anywhere, it counts the log as "unknown".

Finally, it queues the resulting attribute value for async processing.

## Concurrency & Performance Features
Non-blocking receives: The gRPC handler returns immediately after queueing.

Bounded memory: The channel has a configurable size - when full, new logs get dropped, with error logging.

Thread-safe counting: Uses sync.RWMutex to safely update counters from multiple workers.

Graceful shutdown: Properly drains work and stops all goroutines on shutdown signals.

Backpressure handling: Logs are dropped rather than causing memory issues when the system is overwhelmed.

Output: Every processing interval, you'll see a report.