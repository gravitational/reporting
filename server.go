package reporting

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
	context "golang.org/x/net/context"
)

type reportingServer struct {
	server EventsServer
}

func NewServer() EventsServer {
	return &reportingServer{}
}

func (s *reportingServer) Record(ctx context.Context, events *RawEvents) (*empty.Empty, error) {
	for _, e := range events.Events {
		event, err := FromRawEvent(*e)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		bytes, err := json.Marshal(event)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		fmt.Printf("received event: %s\n", bytes)
	}
	return &empty.Empty{}, nil
}
