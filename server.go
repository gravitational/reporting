package reporting

import (
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
	context "golang.org/x/net/context"
)

// ServerConfig defines the reporting server config
type ServerConfig struct {
	// Sinks is the list of event sinks
	Sinks []Sink
}

// NewServer returns a new reporting gRPC server
func NewServer(config ServerConfig) *server {
	return &server{
		ServerConfig: config,
	}
}

type server struct {
	ServerConfig
}

// Record accepts events over gRPC and saves them in the configured sinks
func (s *server) Record(ctx context.Context, grpcEvents *GRPCEvents) (*empty.Empty, error) {
	events, err := FromGRPCEvents(*grpcEvents)
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
