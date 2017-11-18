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

package server

import (
	"context"
	"strings"
	"time"

	"github.com/gravitational/reporting/types"

	"cloud.google.com/go/bigquery"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// Sink defines an event sink interface
type Sink interface {
	// Put saves a series of events
	Put([]types.Event) error
}

// NewLogSink returns a new sink that just logs events
func NewLogSink() *logSink {
	return &logSink{}
}

type logSink struct{}

// Put logs provided events
func (s *logSink) Put(events []types.Event) error {
	for _, event := range events {
		log.WithFields(log.Fields(map[string]interface{}{
			"event": event,
		})).Info("logSink")
	}
	return nil
}

// NewChannelSink returns a new sink that submits events into the provided channel
func NewChannelSink(ch chan types.Event) *chanSink {
	return &chanSink{ch: ch}
}

type chanSink struct {
	ch chan types.Event
}

// Put submits events into the channel the sink was initialized with
func (s *chanSink) Put(events []types.Event) error {
	for _, event := range events {
		s.ch <- event
	}
	return nil
}

// BigQueryConfig is config for Google BigQuery sink
type BigQueryConfig struct {
	// ProjectID is the GCP project ID. Note that proper authentication should
	// be setup as described in
	// https://cloud.google.com/docs/authentication/getting-started
	ProjectID string `json:"projectID"`
}

// Check makes sure that BigQuery sink config is valid
func (c BigQueryConfig) Check() error {
	if c.ProjectID == "" {
		return trace.BadParameter("bigquery config is missing project id")
	}
	return nil
}

// NewBigQuerySink returns a new Google BigQuery events sink
func NewBigQuerySink(config BigQueryConfig) (*bigQuerySink, error) {
	err := config.Check()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	client, err := bigquery.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	bigQuery := &bigQuerySink{client: client}
	err = bigQuery.initSchema()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return bigQuery, nil
}

type bigQuerySink struct {
	client *bigquery.Client
}

// Put saves a series of events into Google BigQuery
func (q *bigQuerySink) Put(events []types.Event) error {
	uploader := q.client.Dataset(bqDatasetName).Table(bqTableName).Uploader()
	// in case of persistent error the call will run indefinitely so
	// pass a context with timeout to prevent hanging calls
	ctx, cancel := context.WithTimeout(context.Background(), bqUploadTimeout)
	defer cancel() // release resources if operation completed before timeout
	err := uploader.Put(ctx, eventsToStructSavers(events))
	if err != nil {
		if pme, ok := err.(bigquery.PutMultiError); ok {
			var errors []error
			for _, err := range pme {
				errors = append(errors, &err)
			}
			return trace.NewAggregate(errors...)
		}
		return trace.Wrap(err)
	}
	return nil
}

// initSchema initializes the dataset and table in Google BigQuery
func (q *bigQuerySink) initSchema() error {
	dataset := q.client.Dataset(bqDatasetName)
	err := dataset.Create(context.Background(), nil)
	if err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			return trace.Wrap(err)
		}
		log.Debugf("dataset %q already exists", bqDatasetName)
	}
	table := dataset.Table(bqTableName)
	err = table.Create(context.Background(), &bigquery.TableMetadata{
		Schema: tableSchema,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			return trace.Wrap(err)
		}
		log.Debugf("table %q already exists", bqTableName)
	}
	return nil
}

// eventsToStructSavers converts a slice of events into the format accepted by
// the BigQuery client with proper schema
func eventsToStructSavers(events []types.Event) []*bigquery.StructSaver {
	savers := make([]*bigquery.StructSaver, len(events))
	for i, event := range events {
		saver, err := eventToStructSaver(event)
		if err != nil {
			log.Warnf(trace.DebugReport(err))
		} else {
			savers[i] = saver
		}
	}
	return savers
}

// eventToStructSaver converts a single event to a BigQuery struct saver
func eventToStructSaver(event types.Event) (*bigquery.StructSaver, error) {
	switch e := event.(type) {
	case *types.ServerEvent:
		return &bigquery.StructSaver{
			Schema:   tableSchema,
			InsertID: e.Spec.ID,
			Struct: bqServerEvent{
				Type:      e.GetName(),
				Action:    e.Spec.Action,
				AccountID: e.Spec.AccountID,
				ServerID:  e.Spec.ServerID,
				Time:      e.GetMetadata().Created.Unix(),
			},
		}, nil
	case *types.UserEvent:
		return &bigquery.StructSaver{
			Schema:   tableSchema,
			InsertID: e.Spec.ID,
			Struct: bqUserEvent{
				Type:      e.GetName(),
				Action:    e.Spec.Action,
				AccountID: e.Spec.AccountID,
				UserID:    e.Spec.UserID,
				Time:      e.GetMetadata().Created.Unix(),
			},
		}, nil
	default:
		return nil, trace.BadParameter("unsupported event type %T: %v", e, e)
	}
}

// bqServerEvents represents BigQuery server event schema
type bqServerEvent struct {
	// Type is the event type
	Type string
	// Action is the event action
	Action string
	// AccountID is ID of account that triggered the event
	AccountID string
	// ServerID is ID of server that triggered the event
	ServerID string
	// Time is the event timestamp
	Time int64
}

// bqUserEvent represents BigQuery user event schema
type bqUserEvent struct {
	// Type is the event type
	Type string
	// Action is the event action
	Action string
	// AccountID is ID of account that triggered the event
	AccountID string
	// UserID is ID of user that triggered the event
	UserID string
	// Time is the event timestamp
	Time int64
}

// tableSchema describes BigQuery events table schema
var tableSchema = bigquery.Schema{
	{
		Name:     "type",
		Required: true,
		Type:     bigquery.StringFieldType,
	},
	{
		Name:     "action",
		Required: true,
		Type:     bigquery.StringFieldType,
	},
	{
		Name:     "accountID",
		Required: true,
		Type:     bigquery.StringFieldType,
	},
	{
		Name:     "time",
		Required: true,
		Type:     bigquery.TimestampFieldType,
	},
	{
		Name: "serverID",
		Type: bigquery.StringFieldType,
	},
	{
		Name: "userID",
		Type: bigquery.StringFieldType,
	},
}

const (
	// bqDatasetName is the BigQuery dataset name
	bqDatasetName = "houston"
	// bqTableName is the BigQuery events table name
	bqTableName = "events"
	// bqUploadTimeout is how long the upload method should retry in case of failures
	bqUploadTimeout = 10 * time.Second
)
