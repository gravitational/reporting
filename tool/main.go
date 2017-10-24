package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	clt "github.com/gravitational/reporting/lib/client"
	evt "github.com/gravitational/reporting/lib/events"
	grp "github.com/gravitational/reporting/lib/grpc"
	srv "github.com/gravitational/reporting/lib/server"
	snk "github.com/gravitational/reporting/lib/sink"

	"github.com/gravitational/license/authority"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	mode = flag.String("mode", "", "server or client")
	port = flag.String("port", "10000", "server port")
	data = flag.String("data", "", "metric data to send")
	cert = flag.String("cert", "", "client certificate")
)

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&trace.TextFormatter{
		TextFormatter: log.TextFormatter{FullTimestamp: true},
	})
	flag.Parse()
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
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
	switch *mode {
	case "server":
		listener, err := net.Listen("tcp", fmt.Sprintf(":%v", *port))
		if err != nil {
			return trace.Wrap(err)
		}
		bq, err := snk.NewBigQuery(snk.BigQueryConfig{
			ProjectID: "kubeadm-167321",
		})
		if err != nil {
			return trace.Wrap(err)
		}
		server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
		grp.RegisterEventsServiceServer(server, srv.New(srv.Config{
			Sinks: []snk.Sink{bq},
		}))
		err = server.Serve(listener)
		if err != nil {
			return trace.Wrap(err)
		}
	case "client":
		client, err := clt.New(
			context.TODO(), clt.Config{
				ServerAddr:  fmt.Sprintf("localhost:%v", *port),
				Certificate: cert,
			})
		if err != nil {
			return trace.Wrap(err)
		}
		for {
			client.Record(&evt.ServerEvent{
				Action:   evt.EventActionLogin,
				ServerID: "xxx",
				Time:     time.Now(),
			})
			time.Sleep(5 * time.Second)
		}
	default:
		return trace.BadParameter("unknown mode")
	}
	return nil
}
