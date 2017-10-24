package reporting

import (
	"context"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/cloudflare/cfssl/log"
	"github.com/gravitational/trace"
)

type Sink interface {
	Put([]Event) error
}

type bigQuery struct {
	client *bigquery.Client
}

type BigQueryConfig struct {
	ProjectID string `json:"projectID"`
}

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

func (q *bigQuery) Put(events []Event) error {
	uploader := q.client.Dataset(datasetName).Table(tableName).Uploader()
	err := uploader.Put(context.Background(), events)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (q *bigQuery) initSchema() error {
	dataset := q.client.Dataset(datasetName)
	err := dataset.Create(context.Background(), nil)
	if err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			return trace.Wrap(err)
		}
		log.Debugf("dataset %q already exists", datasetName)
	}
	schema, err := bigquery.InferSchema(&Event{})
	if err != nil {
		return trace.Wrap(err)
	}
	table := dataset.Table(tableName)
	err = table.Delete(context.Background())
	if err != nil {
		return trace.Wrap(err)
	}
	err = table.Create(context.Background(), &bigquery.TableMetadata{
		Schema: schema,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

const (
	datasetName = "houston"
	tableName   = "events"
)
