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

package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gravitational/reporting"

	"github.com/gravitational/trace"
)

// Notification represents a user notification that, for example,
// is sent from control plane to teleport, implements Event
type Notification struct {
	// Kind is resource kind, for notifications it is "notification"
	Kind string `json:"kind"`
	// Version is the notification resource version
	Version string `json:"version"`
	// Metadata is the notification metadata
	Metadata Metadata `json:"metadata"`
	// Spec is the notification spec
	Spec NotificationSpec `json:"spec"`
}

// NotificationSpec is the notification specification
type NotificationSpec struct {
	// AccountID is the account ID the notification is for
	AccountID string `json:"accountID"`
	// Severity is the notification severity, info, warning or error
	Severity string `json:"severity"`
	// Text is the notification plain text
	Text string `json:"text"`
	// HTML is the notification HTML
	HTML string `json:"html"`
}

// NewNotification returns a new notification with specified name and spec
func NewNotification(name string, spec NotificationSpec) *Notification {
	return &Notification{
		Kind:    reporting.KindNotification,
		Version: reporting.ResourceVersion,
		Metadata: Metadata{
			Name:    name,
			Created: time.Now().UTC(),
		},
		Spec: spec,
	}
}

// GetName returns the notification name (type)
func (n *Notification) GetName() string { return n.Metadata.Name }

// GetMetadata returns the notification metadata
func (n *Notification) GetMetadata() Metadata { return n.Metadata }

// SetAccountID sets the notification account ID
func (n *Notification) SetAccountID(id string) {
	n.Spec.AccountID = id
}

// UnmarshalNotification unmarshals notification with schema validation
func UnmarshalNotification(bytes []byte) (*Notification, error) {
	var header eventHeader
	if err := json.Unmarshal(bytes, &header); err != nil {
		return nil, trace.Wrap(err)
	}
	if header.Kind != reporting.KindNotification {
		return nil, trace.BadParameter("expected kind %q, got %q",
			reporting.KindNotification, header.Kind)
	}
	if header.Version != reporting.ResourceVersion {
		return nil, trace.BadParameter("expected resource version %q, got %q",
			reporting.ResourceVersion, header.Version)
	}
	var notification Notification
	err := unmarshalWithSchema(
		getNotificationSchema(), bytes, &notification)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &notification, nil
}

// notificationSchema is the notification spec schema
const notificationSchema = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["accountID", "severity", "text", "html"],
  "properties": {
    "accountID": {"type": "string"},
    "severity": {"type": "string"},
    "text": {"type": "string"},
    "html": {"type": "string"}
  }
}`

// getNotificationSchema returns full notification schema
func getNotificationSchema() string {
	return fmt.Sprintf(schemaTemplate, notificationSchema)
}
