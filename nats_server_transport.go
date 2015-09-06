package thrift_nats

import (
	"errors"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

var errTransportInterrupted = errors.New("Transport Interrupted")

type TServerNATS struct {
	conn      *nats.Conn
	accepted  chan struct{}
	transport thrift.TTransport
	mu        sync.RWMutex
	listening bool
}

func NewTServerNATS(conn *nats.Conn) *TServerNATS {
	return &TServerNATS{conn: conn}
}

func (t *TServerNATS) Listen() error {
	t.listening = true
	return nil
}

func (t *TServerNATS) Accept() (thrift.TTransport, error) {
	return nil, errors.New("Use AcceptNATS")
}

func (t *TServerNATS) AcceptNATS(listenTo, replyTo string) thrift.TTransport {
	return NewTNATS(t.conn, listenTo, replyTo, 1024*1024)
}

func (t *TServerNATS) IsListening() bool {
	return t.listening
}

func (t *TServerNATS) Close() error {
	return nil
}

func (t *TServerNATS) Interrupt() error {
	return nil
}
