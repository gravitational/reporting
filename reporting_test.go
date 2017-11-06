/*
Copyright 2017 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reporting

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/cloudflare/cfssl/csr"
	"github.com/gravitational/license/authority"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	check "gopkg.in/check.v1"
)

func TestReporting(t *testing.T) { check.TestingT(t) }

type ReportingSuite struct {
	client   Client
	eventsCh chan Event
}

var _ = check.Suite(&ReportingSuite{})

func (r *ReportingSuite) SetUpSuite(c *check.C) {
	r.eventsCh = make(chan Event, 10)
	serverAddr := startTestServer(c, r.eventsCh)
	r.client = getTestClient(c, serverAddr)
}

func (r *ReportingSuite) TestReporting(c *check.C) {
	events := []Event{
		NewServerLoginEvent(uuid.New()),
		NewUserLoginEvent(uuid.New()),
	}
	for _, event := range events {
		r.client.Record(event)
	}
	var received []Event
	for i := 0; i < len(events); i++ {
		select {
		case e := <-r.eventsCh:
			received = append(received, e)
		case <-time.After(testTimeout):
			c.Fatal("timeout waiting for events")
		}
	}
	c.Assert(len(received), check.Equals, len(events))
	c.Assert(received, check.DeepEquals, events)
}

// startTestServer starts gRPC events server that will be submitting events
// into the provided channel, and returns the server address
func startTestServer(c *check.C, ch chan Event) (addr string) {
	// generate certificate authority
	ca, err := authority.GenerateSelfSignedCA(csr.CertificateRequest{CN: "localhost"})
	c.Assert(err, check.IsNil)
	cert, err := tls.X509KeyPair(ca.CertPEM, ca.KeyPEM)
	c.Assert(err, check.IsNil)
	// start gRPC test server
	l, err := net.Listen("tcp", "localhost:0")
	c.Assert(err, check.IsNil)
	server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
	RegisterEventsServiceServer(server, NewServer(ServerConfig{
		Sinks: []Sink{NewChannelSink(ch)},
	}))
	go server.Serve(l)
	return l.Addr().String()
}

// getTestClient returns a new gRPC events client for the provided server address
func getTestClient(c *check.C, addr string) Client {
	client, err := NewClient(context.Background(), ClientConfig{
		ServerAddr: addr,
		Insecure:   true,
	})
	c.Assert(err, check.IsNil)
	return client
}
