package client

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/gravitational/reporting/lib/events"
	"github.com/gravitational/reporting/lib/grpc"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	grpcapi "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type client struct {
	sync.Mutex
	client   grpc.EventsServiceClient
	eventsCh chan events.Event
	events   []events.Event
	ctx      context.Context
}

// Config defines the reporting client config
type Config struct {
	// ServerAddr is the address of the reporting gRPC server
	ServerAddr string
	// ServerName is the SNI server name
	ServerName string
	// Certificate is the client certificate to authenticate with
	Certificate tls.Certificate
}

// Client defines the reporting client interface
type Client interface {
	// Record records an event
	Record(events.Event)
}

// New returns a new reporting gRPC client
func New(ctx context.Context, config Config) (*client, error) {
	conn, err := grpcapi.Dial(config.ServerAddr,
		grpcapi.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				ServerName:         config.ServerName,
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{config.Certificate},
			})))
	if err != nil {
		return nil, trace.Wrap(err)
	}

	client := &client{
		client:   grpc.NewEventsServiceClient(conn),
		eventsCh: make(chan events.Event, FlushCount),
		ctx:      ctx,
	}

	go client.receiveAndFlushEvents()

	return client, nil
}

// Record records an event. Note that the client accumulates events in memory and
// flushes them every once in a while
func (c *client) Record(event events.Event) {
	select {
	case c.eventsCh <- event:
		log.Debugf("queued %v", event)
	default:
		log.Warnf("events channel is full, discarding %v", event)
	}
}

// receiveAndFlushEvents receives events on a channel, accumulates them in memory
// and flushes them once a certain number has been accumulated, or certain amount
// of time has passed
func (c *client) receiveAndFlushEvents() {
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

// flush flushes all accumulated events
func (c *client) flush() error {
	if len(c.events) == 0 {
		return nil
	}

	var grpcEvents grpc.Events
	for _, event := range c.events {
		grpcEvent, err := events.ToGrpcEvent(event)
		if err != nil {
			return trace.Wrap(err)
		}
		grpcEvents.Events = append(
			grpcEvents.Events, grpcEvent)
	}

	_, err := c.client.Record(context.TODO(), &grpcEvents)
	if err != nil {
		return trace.Wrap(err)
	}

	log.Debugf("flushed %v events", len(c.events))
	c.events = []events.Event{}
	return nil
}

const (
	// FlushInterval is how often the flush happens
	FlushInterval = 3 * time.Second
	// FlushCount is the number of events to accumulate before flush triggers
	FlushCount = 1
)
