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
	attributeKey          = flag.String("attributeKey", "foo", "The attribute key to use for counting logs")
	processingInterval    = flag.Duration("processingInterval", 10*time.Second, "The interval at which logs are processed")
	bufferSize            = flag.Int("bufferSize", 1000, "The size of the buffer for incoming logs, must be greater than 0")
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

	processor, err := newLogsProcessor(*attributeKey, *processingInterval, *bufferSize, *numberOfWorkers)

	if err != nil {
		return err
	}

	svr := newServer(processor)

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan

		slog.InfoContext(ctx, "Received shutdown signal")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), *shutdownTimeout)

		defer shutdownCancel()

		if err := processor.shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(ctx, "Error shutting down processor", "error", err)
		}

		grpcServer.GracefulStop()

		cancel()
	}()

	collogspb.RegisterLogsServiceServer(grpcServer, svr)

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
