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
	"testing"

	"github.com/gravitational/reporting"

	"github.com/pborman/uuid"
	check "gopkg.in/check.v1"
)

func TestTypes(t *testing.T) { check.TestingT(t) }

type TypesSuite struct{}

var _ = check.Suite(&TypesSuite{})

func (r *TypesSuite) TestNotifications(c *check.C) {
	n1 := NewNotification(
		reporting.NotificationTypeUsage,
		NotificationSpec{
			AccountID: uuid.New(),
			Severity:  "warning",
			Text:      "Usage limit exceeded",
			HTML:      "<div>Usage limit exceeded</div>",
		})
	n2 := NewNotification(
		reporting.NotificationTypeTOS,
		NotificationSpec{
			AccountID: uuid.New(),
			Severity:  "error",
			Text:      "Terms of service violation",
			HTML:      "<div>Terms of service violation</div>",
		})
	for _, n := range []Notification{*n1, *n2} {
		bytes, err := json.Marshal(n)
		c.Assert(err, check.IsNil)
		var unmarshaled Notification
		err = json.Unmarshal(bytes, &unmarshaled)
		c.Assert(err, check.IsNil)
		c.Assert(unmarshaled, check.DeepEquals, n)
	}
}
