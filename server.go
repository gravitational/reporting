package reporting

import (
	"fmt"
	"io"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
)

type reportingServer struct {
	server EventsServer
}

func NewServer() EventsServer {
	return &reportingServer{}
}

func (s *reportingServer) Record(stream Events_RecordServer) error {
	for {
		rawEvent, err := stream.Recv()
		if err == io.EOF {
			return trace.Wrap(stream.SendAndClose(&empty.Empty{}))
		}
		if err != nil {
			return trace.Wrap(err)
		}
		event, err := FromRawEvent(*rawEvent)
		if err != nil {
			return trace.Wrap(err)
		}
		fmt.Printf("received event: %v\n", event)
	}
	return nil
}
