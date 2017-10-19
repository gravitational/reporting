package reporting

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/license"
	"github.com/gravitational/trace"
	context "golang.org/x/net/context"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type reportingServer struct {
	server EventsServer
}

func NewServer() EventsServer {
	return &reportingServer{}
}

func (s *reportingServer) Record(ctx context.Context, events *RawEvents) (*empty.Empty, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, trace.BadParameter("failed to retrieve grpc peer info from %v", ctx)
	}
	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, trace.BadParameter("expected TLSInfo, got %T", peer.AuthInfo)
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) != 1 {
		return nil, trace.BadParameter("expected 1 client certificate, got %v", len(certs))
	}
	license, err := license.ParseLicenseFromX509(certs[0])
	if err != nil {
		return nil, trace.Wrap(err)
	}
	for _, e := range events.Events {
		event, err := FromRawEvent(*e)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		bytes, err := json.Marshal(event)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		fmt.Printf("received event: %s, license payload: %v\n", bytes, license.GetPayload())
	}
	return &empty.Empty{}, nil
}
