package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/gravitational/reporting"
	"github.com/gravitational/trace"
)

var (
	mode = flag.String("mode", "", "server or client")
	port = flag.String("port", "10000", "server port")
	data = flag.String("data", "", "metric data to send")
)

func main() {
	flag.Parse()
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	switch *mode {
	case "server":
		listener, err := net.Listen("tcp", fmt.Sprintf(":%v", *port))
		if err != nil {
			return trace.Wrap(err)
		}
		server := grpc.NewServer()
		reporting.RegisterEventsServer(server, reporting.NewServer())
		err = server.Serve(listener)
		if err != nil {
			return trace.Wrap(err)
		}
	case "client":
		client, err := reporting.NewClient(
			context.TODO(), fmt.Sprintf("localhost:%v", *port))
		if err != nil {
			return trace.Wrap(err)
		}
		client.Record(reporting.Event{
			Type:      reporting.EventTypeNodeAccessed,
			Timestamp: time.Now(),
			NodeAccessed: &reporting.NodeAccessed{
				NodeHash: *data,
			},
		})
		time.Sleep(10 * time.Second)
	default:
		return trace.BadParameter("unknown mode")
	}
	return nil
}
