package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/gravitational/reporting/lib"
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
		listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", *port))
		if err != nil {
			return trace.Wrap(err)
		}
		server := grpc.NewServer()
		lib.RegisterMetricsServer(server, lib.NewServer())
		err = server.Serve(listener)
		if err != nil {
			return trace.Wrap(err)
		}
	case "client":
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%v", *port), grpc.WithInsecure())
		if err != nil {
			return trace.Wrap(err)
		}
		defer conn.Close()
		client := lib.NewClient(conn)
		err = client.Record(*data)
		if err != nil {
			return trace.Wrap(err)
		}
	default:
		return trace.BadParameter("unknown mode")
	}
	return nil
}
