package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter  = otel.Meter(name)
	logger = otelslog.NewLogger(name)

	logsReceivedCounter metric.Int64Counter

	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
	attributeKey          = flag.String("attributeKey", "foo", "The attribute key to use for counting logs, must not be empty")
	processingInterval    = flag.Duration("processingInterval", 10*time.Second, "The interval at which logs are processed, must be greater than 0")
	attributeChanSize     = flag.Int("attributeChanSize", 1000, "Size of the channel for receiving attributes, must be greater than 0")
	numberOfWorkers       = flag.Int("numberOfWorkers", 2, "Number of workers to process logs concurrently, must be greater than 0")
	shutdownTimeout       = flag.Duration("shutdownTimeout", 30*time.Second, "Timeout for graceful shutdown of the server, must be greater than 0")
)

func init() {
	var err error

	logsReceivedCounter, err = meter.Int64Counter(
		"com.dash0.homeexercise.logs.received",
		metric.WithDescription("The number of logs received by otlp-log-processor-backend"),
		metric.WithUnit("{log}"),
	)

	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)

	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())

	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	// create a context with a cancel function to handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	slog.DebugContext(ctx, "Starting listener", slog.String("listenAddr", *listenAddr))

	listener, err := net.Listen("tcp", *listenAddr)

	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)

	processor, err := newLogsProcessor(logger, os.Stdout, *attributeKey, *processingInterval, *attributeChanSize, *numberOfWorkers)

	if err != nil {
		return err
	}

	svr := newServer(processor)

	// set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)

	// notify the channel on interrupt or termination signals
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// run the shutdown logic in a separate goroutine
	go func() {
		// close the channel after the goroutine completes
		defer close(sigChan)

		// wait for a signal to shut down
		<-sigChan

		slog.InfoContext(ctx, "Received shutdown signal")

		// initiate shutdown of the processor
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), *shutdownTimeout)

		defer shutdownCancel()

		if err := processor.shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "Error shutting down processor", "error", err)
		}

		// gracefully stop the gRPC server
		grpcServer.GracefulStop()

		// close the listener to stop accepting new connections
		cancel()
	}()

	collogspb.RegisterLogsServiceServer(grpcServer, svr)

	// register reflection service for gRPC server introspection
	reflection.Register(grpcServer)

	serverErr := make(chan error, 1)

	go func() {
		slog.InfoContext(ctx, "Starting gRPC server", "address", *listenAddr)

		serverErr <- grpcServer.Serve(listener)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			slog.ErrorContext(ctx, "gRPC server error", "error", err)

			return err
		}
	case <-ctx.Done():
		slog.InfoContext(ctx, "Application shutdown complete")
	}

	return nil
}
