package thrift_nats

import (
	"errors"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

var errTransportInterrupted = errors.New("Transport Interrupted")

type natsServerTransport struct {
	conn      *nats.Conn
	accepted  chan struct{}
	transport thrift.TTransport
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

func (n *natsServerTransport) AcceptNATS(listenTo, replyTo string) thrift.TTransport {
	return NewNATSTransport(n.conn, listenTo, replyTo, "", 0, -1)
}

func (n *natsServerTransport) IsListening() bool {
	return n.listening
}

func (n *natsServerTransport) Close() error {
	return nil
}

func (n *natsServerTransport) Interrupt() error {
	return nil
}
