package sink

import (
	"context"
	"strings"

	"github.com/gravitational/reporting/lib/events"

	"cloud.google.com/go/bigquery"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// Sink defines an event sink interface
type Sink interface {
	// Put saves a series of events
	Put([]events.Event) error
}

type bigQuery struct {
	client *bigquery.Client
}

// BigQueryConfig is config for Google BigQuery sink
type BigQueryConfig struct {
	// ProjectID is the GCP project ID. Note that proper authentication should be setup
	// as described in https://cloud.google.com/docs/authentication/getting-started
	ProjectID string `json:"projectID"`
}

// NewBigQuery returns a new Google BigQuery events sink
func NewBigQuery(config BigQueryConfig) (*bigQuery, error) {
	client, err := bigquery.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	bigQuery := &bigQuery{client: client}
	err = bigQuery.initSchema()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return bigQuery, nil
}

// Put saves a series of events into Google BigQuery
func (q *bigQuery) Put(events []events.Event) error {
	uploader := q.client.Dataset(datasetName).Table(tableName).Uploader()
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
func (q *bigQuery) initSchema() error {
	dataset := q.client.Dataset(datasetName)
	err := dataset.Create(context.Background(), nil)
	if err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			return trace.Wrap(err)
		}
		log.Debugf("dataset %q already exists", datasetName)
	}
	table := dataset.Table(tableName)
	err = table.Delete(context.Background())
	if err != nil {
		return trace.Wrap(err)
	}
	err = table.Create(context.Background(), &bigquery.TableMetadata{
		Schema: tableSchema,
	})
	if err != nil {
		return trace.Wrap(err)
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

const (
	// datasetName is the BigQuery dataset name
	datasetName = "houston"
	// tableName is the BigQuery events table name
	tableName = "events"
)
