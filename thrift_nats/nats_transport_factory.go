/*
Copyright 2015 Workiva

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

package thrift_nats

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

// NATSTransportFactory returns a new thrift TTransport which uses the NATS
// messaging system as the underlying transport. This TTransport can only be
// used with NATSServer.
func NATSTransportFactory(conn *nats.Conn, subject string,
	timeout, readTimeout time.Duration) (thrift.TTransport, error) {

	msg, inbox, err := connect(conn, subject, timeout)
	if err != nil {
		return nil, err
	}
	if msg.Reply == "" {
		return nil, errors.New("thrift_nats: no reply subject on connect")
	}

	heartbeatAndDeadline := strings.Split(string(msg.Data), " ")
	if len(heartbeatAndDeadline) != 2 {
		return nil, errors.New("thrift_nats: invalid connect message")
	}
	heartbeat := heartbeatAndDeadline[0]
	deadline, err := strconv.ParseInt(heartbeatAndDeadline[1], 10, 64)
	if err != nil {
		return nil, err
	}
	var interval time.Duration
	if deadline > 0 {
		deadline = deadline - (deadline / 4)
		interval = time.Millisecond * time.Duration(deadline)
	}

	return NewNATSTransport(conn, inbox, msg.Reply, heartbeat, readTimeout, interval), nil
}

func connect(conn *nats.Conn, subj string, timeout time.Duration) (*nats.Msg, string, error) {
	inbox := nats.NewInbox()
	s, err := conn.Subscribe(inbox, nil)
	if err != nil {
		return nil, "", err
	}
	s.AutoUnsubscribe(1)
	err = conn.PublishRequest(subj, inbox, nil)
	if err != nil {
		return nil, "", err
	}
	msg, err := s.NextMsg(timeout)
	if err != nil {
		return nil, "", err
	}
	return msg, inbox, nil
}
