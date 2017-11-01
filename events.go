package reporting

import (
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gravitational/trace"
	"github.com/pborman/uuid"
)

// Event defines an interface all event types should implement
type Event interface {
	// Type returns event type
	Type() string
	// SetAccountID sets the event account id
	SetAccountID(string)
}

// ServerEvent represents server-related event, such as "logged into server"
type ServerEvent struct {
	// ID is event ID, may be used for de-duplication
	ID string `json:"id"`
	// Action is event action, such as "login"
	Action string `json:"action"`
	// AccountID is ID of account that triggered the event
	AccountID string `json:"accountID"`
	// ServerID is anonymized ID of server that triggered the event
	ServerID string `json:"serverID"`
	// Time is the event timestamp
	Time time.Time `json:"time"`
}

// NewServerLoginEvent creates an instance of "server login" event
func NewServerLoginEvent(serverID string) *ServerEvent {
	return &ServerEvent{
		ID:       uuid.New(),
		Action:   EventActionLogin,
		ServerID: serverID,
		Time:     time.Now().UTC(),
	}
}

// Type returns the event type
func (e *ServerEvent) Type() string { return EventTypeServer }

// SetAccountID sets the event account id
func (e *ServerEvent) SetAccountID(id string) {
	e.AccountID = id
}

// Save implements bigqquery.ValueSaver
func (e *ServerEvent) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"type":      e.Type(),
		"action":    e.Action,
		"accountID": e.AccountID,
		"serverID":  e.ServerID,
		"time":      e.Time.Unix(),
	}, e.ID, nil
}

// UserEvent represents user-related event, such as "user logged in"
type UserEvent struct {
	// ID is event ID, may be used for de-duplication
	ID string `json:"id"`
	// Action is event action, such as "login"
	Action string `json:"action"`
	// AccountID is ID of account that triggered the event
	AccountID string `json:"accountID"`
	// UserID is anonymized ID of user that triggered the event
	UserID string `json:"userID"`
	// Time is the event timestamp
	Time time.Time `json:"time"`
}

// NewUserLoginEvent creates an instance of "user login" event
func NewUserLoginEvent(userID string) *UserEvent {
	return &UserEvent{
		ID:     uuid.New(),
		Action: EventActionLogin,
		UserID: userID,
		Time:   time.Now().UTC(),
	}
}

// Type returns the event type
func (e *UserEvent) Type() string { return EventTypeUser }

// SetAccountID sets the event account id
func (e *UserEvent) SetAccountID(id string) {
	e.AccountID = id
}

// Save implements bigquery.ValueSaver
func (e *UserEvent) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"type":      e.Type(),
		"action":    e.Action,
		"accountID": e.AccountID,
		"userID":    e.UserID,
		"time":      e.Time.Unix(),
	}, e.ID, nil
}

// ToGRPCEvent converts provided event to the format used by gRPC server/client
func ToGRPCEvent(event Event) (*GRPCEvent, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &GRPCEvent{
		Type: event.Type(),
		Data: payload,
	}, nil
}

// FromGRPCEvent converts event from the format used by gRPC server/client
func FromGRPCEvent(grpcEvent GRPCEvent) (Event, error) {
	switch grpcEvent.Type {
	case EventTypeServer:
		var event ServerEvent
		if err := json.Unmarshal(grpcEvent.Data, &event); err != nil {
			return nil, trace.Wrap(err)
		}
		return &event, nil
	case EventTypeUser:
		var event UserEvent
		if err := json.Unmarshal(grpcEvent.Data, &event); err != nil {
			return nil, trace.Wrap(err)
		}
		return &event, nil
	default:
		return nil, trace.BadParameter("unknown event type %q", grpcEvent.Type)
	}
}

// FromGRPCEvents converts a series of events from the format used by gRPC server/client
func FromGRPCEvents(grpcEvents GRPCEvents) ([]Event, error) {
	var events []Event
	for _, grpcEvent := range grpcEvents.Events {
		event, err := FromGRPCEvent(*grpcEvent)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		events = append(events, event)
	}
	return events, nil
}
