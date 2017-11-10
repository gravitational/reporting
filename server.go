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
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
	context "golang.org/x/net/context"
)

// ServerConfig defines the reporting server config
type ServerConfig struct {
	// Sinks is the list of event sinks
	Sinks []Sink
}

// NewServer returns a new reporting gRPC server
func NewServer(config ServerConfig) *server {
	return &server{
		ServerConfig: config,
	}
}

type server struct {
	ServerConfig
}

// Record accepts events over gRPC and saves them in the configured sinks
func (s *server) Record(ctx context.Context, grpcEvents *GRPCEvents) (*empty.Empty, error) {
	events, err := FromGRPCEvents(*grpcEvents)
	if err != nil {
		log.Errorf(trace.DebugReport(err))
		return nil, trace.Wrap(err)
	}
	for _, sink := range s.Sinks {
		err := sink.Put(events)
		if err != nil {
			log.Error(trace.DebugReport(err))
			return nil, trace.Wrap(err)
		}
	}
	return &empty.Empty{}, nil
}
