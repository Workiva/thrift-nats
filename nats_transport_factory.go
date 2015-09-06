package thrift_nats

import (
	"errors"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

// NATSTransportFactory returns a new thrift TTransport which uses the NATS
// messaging system as the underlying transport. This TTransport can only be
// used with NATSServer.
func NATSTransportFactory(conn *nats.Conn, subject string,
	timeout time.Duration) (thrift.TTransport, error) {

	reply, inbox, err := request(conn, subject, timeout)
	if err != nil {
		return nil, err
	}
	if reply == "" {
		return nil, errors.New("No reply subject")
	}

	return NewNATSTransport(conn, inbox, reply), nil
}

func request(conn *nats.Conn, subj string, timeout time.Duration) (string, string, error) {
	inbox := nats.NewInbox()
	s, err := conn.Subscribe(inbox, nil)
	if err != nil {
		return "", "", err
	}
	s.AutoUnsubscribe(1)
	err = conn.PublishRequest(subj, inbox, nil)
	if err != nil {
		return "", "", err
	}
	msg, err := s.NextMsg(timeout)
	if err != nil {
		return "", "", err
	}
	s.Unsubscribe()
	return msg.Reply, inbox, nil
}
