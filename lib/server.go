package lib

import (
	"fmt"
	"io"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
)

type reportingServer struct {
}

func NewServer() MetricsServer {
	return &reportingServer{}
}

func (s *reportingServer) Record(stream Metrics_RecordServer) error {
	for {
		metric, err := stream.Recv()
		if err == io.EOF {
			return trace.Wrap(stream.SendAndClose(&empty.Empty{}))
		}
		if err != nil {
			return trace.Wrap(err)
		}
		fmt.Println("got metric:", metric)
	}
	return nil
}
