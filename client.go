package reporting

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type reportingClient struct {
	sync.Mutex
	client   EventsClient
	eventsCh chan Event
	events   []Event
	ctx      context.Context
}

type ClientConfig struct {
	ServerAddr string
	Cert       tls.Certificate
}

type Client interface {
	Record(Event)
}

func NewClient(ctx context.Context, config ClientConfig) (Client, error) {
	conn, err := grpc.Dial(config.ServerAddr,
		grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{config.Cert},
			})))
	if err != nil {
		return nil, trace.Wrap(err)
	}

	client := &reportingClient{
		client:   NewEventsClient(conn),
		eventsCh: make(chan Event, FlushCount),
		ctx:      ctx,
	}

	go client.receiveAndFlushEvents()

	return client, nil
}

func (c *reportingClient) Record(event Event) {
	select {
	case c.eventsCh <- event:
		log.Debugf("queued %v", event)
	default:
		log.Warnf("events channel is full, discarding %v", event)
	}
}

func (c *reportingClient) receiveAndFlushEvents() {
	for {
		select {
		case event := <-c.eventsCh:
			if len(c.events) >= FlushCount {
				err := c.flush()
				if err != nil {
					log.Errorf("events queue full and failed to flush events, discarding %v: %v",
						event, trace.DebugReport(err))
					continue
				}
			}
			c.events = append(c.events, event)

		case <-time.After(FlushInterval):
			err := c.flush()
			if err != nil {
				log.Errorf("failed to flush events: %v",
					trace.DebugReport(err))
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *reportingClient) flush() error {
	if len(c.events) == 0 {
		return nil
	}

	var rawEvents RawEvents
	for _, event := range c.events {
		rawEvent, err := event.ToRawEvent()
		if err != nil {
			return trace.Wrap(err)
		}
		rawEvents.Events = append(
			rawEvents.Events, rawEvent)
	}

	_, err := c.client.Record(context.TODO(), &rawEvents)
	if err != nil {
		return trace.Wrap(err)
	}

	log.Debugf("flushed %v events", len(c.events))
	c.events = []Event{}
	return nil
}

const (
	FlushInterval = 3 * time.Second
	FlushCount    = 10
)
