package reporting

import "time"

const (
	// flushInterval is how often the client flushes accumulated events
	flushInterval = 3 * time.Second
	// flushCount is the number of events to accumulate before flush triggers
	flushCount = 1
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
	// testTimeout is how long to wait for events during tests
	testTimeout = 5 * time.Second
)
