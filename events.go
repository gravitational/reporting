package reporting

import (
	"encoding/json"
	"time"

	"github.com/gravitational/trace"
)

type Event struct {
	Type         string        `json:"type"`
	Timestamp    time.Time     `json:"timestamp"`
	NodeAccessed *NodeAccessed `json:"nodeAccessed,omitempty"`
	UserLoggedIn *UserLoggedIn `json:"userLoggedIn,omitempty"`
}

func (e *Event) ToRawEvent() (*RawEvent, error) {
	var payload []byte
	var err error
	switch e.Type {
	case EventTypeNodeAccessed:
		if e.NodeAccessed == nil {
			return nil, trace.BadParameter("missing NodeAccessed payload")
		}
		payload, err = json.Marshal(e.NodeAccessed)
	case EventTypeUserLoggedIn:
		if e.UserLoggedIn == nil {
			return nil, trace.BadParameter("missing UserLoggedIn payload")
		}
		payload, err = json.Marshal(e.UserLoggedIn)
	default:
		return nil, trace.BadParameter("unknown event type %q", e.Type)
	}
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return &RawEvent{
		Type:      e.Type,
		Data:      payload,
		Timestamp: e.Timestamp.Unix(),
	}, nil
}

func FromRawEvent(e RawEvent) (*Event, error) {
	switch e.Type {
	case EventTypeNodeAccessed:
		var payload NodeAccessed
		err := json.Unmarshal(e.Data, &payload)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		return &Event{
			Type:         e.Type,
			Timestamp:    time.Unix(e.Timestamp, 0),
			NodeAccessed: &payload,
		}, nil
	case EventTypeUserLoggedIn:
		var payload UserLoggedIn
		err := json.Unmarshal(e.Data, &payload)
		if err != nil {
			return nil, trace.Wrap(err)
		}
		return &Event{
			Type:         e.Type,
			Timestamp:    time.Unix(e.Timestamp, 0),
			UserLoggedIn: &payload,
		}, nil
	default:
		return nil, trace.BadParameter("unknown event type %q", e.Type)
	}
}

type NodeAccessed struct {
	NodeHash string `json:"nodeHash"`
}

type UserLoggedIn struct {
	UserHash string `json:"userHash"`
}

const (
	EventTypeNodeAccessed = "nodeAccessed"
	EventTypeUserLoggedIn = "userLoggedIn"
)
