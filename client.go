package reporting

import (
	"context"
	"sync"
	"time"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type reportingClient struct {
	sync.Mutex
	client   EventsClient
	eventsCh chan Event
	events   []Event
	ctx      context.Context
}

type Client interface {
	Record(Event)
}

func NewClient(ctx context.Context, addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, trace.Wrap(err)
	}

	client := &reportingClient{
		client:   NewEventsClient(conn),
		eventsCh: make(chan Event, FlushCount),
		ctx:      ctx,
	}

	go client.receiveEvents()
	go client.periodicFlush()

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

func (c *reportingClient) receiveEvents() {
	for {
		select {
		case event := <-c.eventsCh:
			err := c.onEvent(event)
			if err != nil {
				log.Errorf(trace.DebugReport(err))
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *reportingClient) onEvent(event Event) error {
	c.Lock()
	defer c.Unlock()

	c.events = append(c.events, event)

	if len(c.events) >= FlushCount {
		err := c.flush()
		if err != nil {
			return trace.Wrap(err)
		}
	}

	return nil
}

func (c *reportingClient) periodicFlush() {
	for {
		select {
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
	c.Lock()
	defer c.Unlock()

	log.Debug("flushing events")

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

	c.events = []Event{}
	return nil
}

const (
	FlushInterval = 3 * time.Second
	FlushCount    = 10
)
