package reporting

import (
	"context"

	"google.golang.org/grpc"

	"github.com/gravitational/trace"
)

type reportingClient struct {
	client EventsClient
}

type Client interface {
	Record(Event) error
}

func NewClient(addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return &reportingClient{
		client: NewEventsClient(conn),
	}, nil
}

func (c *reportingClient) Record(e Event) error {
	stream, err := c.client.Record(context.TODO())
	if err != nil {
		return trace.Wrap(err)
	}

	rawEvent, err := e.ToRawEvent()
	if err != nil {
		return trace.Wrap(err)
	}

	err = stream.Send(rawEvent)
	if err != nil {
		return trace.Wrap(err)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}
