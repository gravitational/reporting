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
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// Sink defines an event sink interface
type Sink interface {
	// Put saves a series of events
	Put([]Event) error
}

// NewLogSink returns a new sink that just logs events
func NewLogSink() *logSink {
	return &logSink{}
}

type logSink struct{}

// Put logs provided events
func (s *logSink) Put(events []Event) error {
	for _, event := range events {
		log.WithFields(log.Fields(map[string]interface{}{
			"event": event,
		})).Info("logSink")
	}
	return nil
}

// NewChannelSink returns a new sink that submits events into the provided channel
func NewChannelSink(ch chan Event) *chanSink {
	return &chanSink{ch: ch}
}

type chanSink struct {
	ch chan Event
}

// Put submits events into the channel the sink was initialized with
func (s *chanSink) Put(events []Event) error {
	for _, event := range events {
		s.ch <- event
	}
	return nil
}

// BigQueryConfig is config for Google BigQuery sink
type BigQueryConfig struct {
	// ProjectID is the GCP project ID. Note that proper authentication should be setup
	// as described in https://cloud.google.com/docs/authentication/getting-started
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
func (q *bigQuerySink) Put(events []Event) error {
	uploader := q.client.Dataset(bqDatasetName).Table(bqTableName).Uploader()
	err := uploader.Put(context.Background(), events)
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
