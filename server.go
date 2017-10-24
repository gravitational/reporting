package reporting

import (
	"encoding/json"

	"github.com/cloudflare/cfssl/log"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
	context "golang.org/x/net/context"
)

type reportingServer struct {
	ServerConfig
}

type ServerConfig struct {
	Sinks []Sink
}

func NewServer(config ServerConfig) EventsServer {
	return &reportingServer{
		ServerConfig: config,
	}
}

func (s *reportingServer) Record(ctx context.Context, rawEvents *RawEvents) (*empty.Empty, error) {
	var events []Event
	for _, e := range rawEvents.Events {
		event, err := FromRawEvent(*e)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		bytes, err := json.Marshal(event)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		log.Debugf("received event: %s\n", bytes)
		events = append(events, *event)
	}
	for _, sink := range s.Sinks {
		err := sink.Put(events)
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return &empty.Empty{}, nil
}
