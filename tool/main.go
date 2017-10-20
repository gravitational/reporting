package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/gravitational/license/authority"
	"github.com/gravitational/reporting"
	"github.com/gravitational/trace"
)

var (
	mode = flag.String("mode", "", "server or client")
	port = flag.String("port", "10000", "server port")
	data = flag.String("data", "", "metric data to send")
	cert = flag.String("cert", "", "client certificate")
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
		certAndKey, err := ioutil.ReadFile(*cert)
		if err != nil {
			return trace.Wrap(err)
		}
		certPEM, keyPEM, err := authority.SplitPEM(certAndKey)
		if err != nil {
			return trace.Wrap(err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return trace.Wrap(err)
		}
		client, err := reporting.NewClient(
			context.TODO(), reporting.ClientConfig{
				ServerAddr: fmt.Sprintf("localhost:%v", *port),
				Cert:       cert,
			})
		if err != nil {
			return trace.Wrap(err)
		}
		for {
			client.Record(reporting.Event{
				Type:      reporting.EventTypeNodeAccessed,
				Timestamp: time.Now(),
				NodeAccessed: &reporting.NodeAccessed{
					NodeHash: *data,
				},
			})
			time.Sleep(1 * time.Second)
		}
	default:
		return trace.BadParameter("unknown mode")
	}
	return nil
}
