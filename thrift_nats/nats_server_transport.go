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
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

type natsServerTransport struct {
	conn      *nats.Conn
	listening bool
}

func newNATSServerTransport(conn *nats.Conn) *natsServerTransport {
	return &natsServerTransport{conn: conn}
}

func (n *natsServerTransport) Listen() error {
	n.listening = true
	return nil
}

func (n *natsServerTransport) Accept() (thrift.TTransport, error) {
	return nil, errors.New("Use AcceptNATS")
}

func (n *natsServerTransport) AcceptNATS(listenTo, replyTo string,
	timeout time.Duration) thrift.TTransport {

	return NewNATSTransport(n.conn, listenTo, replyTo, "", timeout, -1)
}

func (n *natsServerTransport) IsListening() bool {
	return n.listening
}

func (n *natsServerTransport) Close() error {
	n.listening = false
	return nil
}

func (n *natsServerTransport) Interrupt() error {
	return nil
}
