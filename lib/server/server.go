package server

import (
	"github.com/gravitational/reporting/lib/events"
	"github.com/gravitational/reporting/lib/grpc"
	"github.com/gravitational/reporting/lib/sink"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
	context "golang.org/x/net/context"
)

type server struct {
	Config
}

// Config defines the reporting server config
type Config struct {
	// Sinks is the list of event sinks
	Sinks []sink.Sink
}

// New returns a new reporting gRPC server
func New(config Config) *server {
	return &server{
		Config: config,
	}
}

// Record accepts events over gRPC and saves them in the configured sinks
func (s *server) Record(ctx context.Context, grpcEvents *grpc.Events) (*empty.Empty, error) {
	events, err := events.FromGrpcEvents(*grpcEvents)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	for _, sink := range s.Sinks {
		err := sink.Put(events)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return &empty.Empty{}, nil
}
