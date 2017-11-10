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

import "time"

const (
	// flushInterval is how often the client flushes accumulated events
	flushInterval = 3 * time.Second
	// flushCount is the number of events to accumulate before flush triggers
	flushCount = 5
	// bqDatasetName is the BigQuery dataset name
	bqDatasetName = "houston"
	// bqTableName is the BigQuery events table name
	bqTableName = "events"
	// EventTypeServer is the server-related event type
	EventTypeServer = "server"
	// EventTypeUser is the user-related event type
	EventTypeUser = "user"
	// EventActionLogin is the event login action
	EventActionLogin = "login"
	// KindEvent is the event resource kind
	KindEvent = "event"
	// ResourceVersion is the current event resource version
	ResourceVersion = "v2"
	// testTimeout is how long to wait for events during tests
	testTimeout = 5 * time.Second
)
