package lib

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/gravitational/trace"
)

type reportingClient struct {
	client MetricsClient
}

type Client interface {
	Record(string) error
}

func NewClient(conn *grpc.ClientConn) Client {
	return &reportingClient{
		client: NewMetricsClient(conn),
	}
}

func (c *reportingClient) Record(nodeHash string) error {
	stream, err := c.client.Record(context.TODO())
	if err != nil {
		return trace.Wrap(err)
	}

	err = stream.Send(&Metric{
		NodeHash:  nodeHash,
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		return trace.Wrap(err)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}
