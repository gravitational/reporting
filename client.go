package reporting

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	grpcapi "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ClientConfig defines the reporting client config
type ClientConfig struct {
	// ServerAddr is the address of the reporting gRPC server
	ServerAddr string
	// ServerName is the SNI server name
	ServerName string
	// Certificate is the client certificate to authenticate with
	Certificate tls.Certificate
	// Insecure is whether the client should skip server cert verification
	Insecure bool
}

// Client defines the reporting client interface
type Client interface {
	// Record records an event
	Record(Event)
}

// NewClient returns a new reporting gRPC client
func NewClient(ctx context.Context, config ClientConfig) (*client, error) {
	conn, err := grpcapi.Dial(config.ServerAddr,
		grpcapi.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				ServerName:         config.ServerName,
				InsecureSkipVerify: config.Insecure,
				Certificates:       []tls.Certificate{config.Certificate},
			})))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	client := &client{
		client:   NewEventsServiceClient(conn),
		eventsCh: make(chan Event, flushCount),
		ctx:      ctx,
	}
	go client.receiveAndFlushEvents()
	return client, nil
}

type client struct {
	client   EventsServiceClient
	eventsCh chan Event
	events   []Event
	ctx      context.Context
}

// Record records an event. Note that the client accumulates events in memory and
// flushes them every once in a while
func (c *client) Record(event Event) {
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
			if len(c.events) >= flushCount {
				err := c.flush()
				if err != nil {
					log.Errorf("events queue full and failed to flush events, discarding %v: %v",
						event, trace.DebugReport(err))
					continue
				}
			}
			c.events = append(c.events, event)
		case <-time.After(flushInterval):
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
	var grpcEvents GRPCEvents
	for _, event := range c.events {
		grpcEvent, err := ToGRPCEvent(event)
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
	c.events = []Event{}
	return nil
}
