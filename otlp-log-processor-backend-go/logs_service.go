package main

import (
	"context"
	"log/slog"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dash0LogsServiceServer struct {
	collogspb.UnimplementedLogsServiceServer

	logsProcessor *logsProcessor
}

func newServer(
	logsProcessor *logsProcessor,
) collogspb.LogsServiceServer {
	return &dash0LogsServiceServer{
		logsProcessor: logsProcessor,
	}
}

func (l *dash0LogsServiceServer) Export(
	ctx context.Context,
	request *collogspb.ExportLogsServiceRequest,
) (*collogspb.ExportLogsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportLogsServiceRequest")

	logsReceivedCounter.Add(ctx, 1)

	if err := l.logsProcessor.processLogs(request); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &collogspb.ExportLogsServiceResponse{}, nil
}
