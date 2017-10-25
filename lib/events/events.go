package events

import (
	"encoding/json"
	"time"

	"github.com/gravitational/reporting/lib/grpc"

	"cloud.google.com/go/bigquery"
	"github.com/gravitational/trace"
)

// Event defines an interface all event types should implement
type Event interface {
	// Type returns event type
	Type() string
}

// ServerEvent represents server-related event, such as "logged into server"
type ServerEvent struct {
	// Action is event action, such as "login"
	Action string `json:"action"`
	// AccountID is ID of account that triggered the event
	AccountID string `json:"accountID"`
	// ServerID is ID of server that triggered the event
	ServerID string `json:"serverID"`
	// Time is the event timestamp
	Time time.Time `json:"time"`
}

// NewServerLoginEvent creates an instance of "server login" event
func NewServerLoginEvent(serverID string) *ServerEvent {
	return &ServerEvent{
		Action:   EventActionLogin,
		ServerID: serverID,
		Time:     time.Now().UTC(),
	}
}

// Type returns the event type
func (e *ServerEvent) Type() string { return EventTypeServer }

// Save implements bigqquery.ValueSaver
func (e *ServerEvent) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"type":      e.Type(),
		"action":    e.Action,
		"accountID": e.AccountID,
		"serverID":  e.ServerID,
		"time":      e.Time.Unix(),
	}, "", nil
}

// UserEvent represents user-related event, such as "user logged in"
type UserEvent struct {
	// Action is event action, such as "login"
	Action string `json:"action"`
	// AccountID is ID of account that triggered the event
	AccountID string `json:"accountID"`
	// UserID is ID of user that triggered the event
	UserID string `json:"userID"`
	// Time is the event timestamp
	Time time.Time `json:"time"`
}

// NewUserLoginEvent creates an instance of "user login" event
func NewUserLoginEvent(userID string) *UserEvent {
	return &UserEvent{
		Action: EventActionLogin,
		UserID: userID,
		Time:   time.Now().UTC(),
	}
}

// Type returns the event type
func (e *UserEvent) Type() string { return EventTypeUser }

// Save implements bigquery.ValueSaver
func (e *UserEvent) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"type":      e.Type(),
		"action":    e.Action,
		"accountID": e.AccountID,
		"userID":    e.UserID,
		"time":      e.Time.Unix(),
	}, "", nil
}

// ToGrpcEvent converts provided event to the format used by gRPC server/client
func ToGrpcEvent(event Event) (*grpc.Event, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &grpc.Event{
		Type: event.Type(),
		Data: payload,
	}, nil
}

// FromGrpcEvent converts event from the format used by gRPC server/client
func FromGrpcEvent(grpcEvent grpc.Event) (Event, error) {
	switch grpcEvent.Type {
	case EventTypeServer:
		var event ServerEvent
		err := json.Unmarshal(grpcEvent.Data, &event)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		return &event, nil
	case EventTypeUser:
		var event UserEvent
		err := json.Unmarshal(grpcEvent.Data, &event)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		return &event, nil
	default:
		return nil, trace.BadParameter("unknown event type %q", grpcEvent.Type)
	}
}

// FromGrpcEvents converts a series of events from the format used by gRPC server/client
func FromGrpcEvents(grpcEvents grpc.Events) ([]Event, error) {
	var events []Event
	for _, grpcEvent := range grpcEvents.Events {
		event, err := FromGrpcEvent(*grpcEvent)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		events = append(events, event)
	}
	return events, nil
}

const (
	// EventTypeServer is the server-related event type
	EventTypeServer = "server"
	// EventTypeUser is the user-related event type
	EventTypeUser = "user"
	// EventActionLogin is the event login action
	EventActionLogin = "login"
)
