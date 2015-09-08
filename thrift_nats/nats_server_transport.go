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

	return NewNATSTransport(n.conn, listenTo, replyTo, timeout, true)
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
