package thrift_nats

import (
	"errors"
	"time"

	"github.com/nats-io/nats"
)

func TNATSFactory(conn *nats.Conn, subject string, timeout time.Duration) (*TNATS, error) {
	msg, inbox, err := request(conn, subject, timeout)
	if err != nil {
		return nil, err
	}
	if msg.Reply == "" {
		return nil, errors.New("No reply subject")
	}

	return NewTNATS(conn, inbox, msg.Reply, 1024*1024), nil
}

func request(conn *nats.Conn, subj string, timeout time.Duration) (m *nats.Msg,
	inbox string, err error) {

	inbox = nats.NewInbox()
	s, err := conn.Subscribe(inbox, nil)
	if err != nil {
		return nil, "", err
	}
	s.AutoUnsubscribe(1)
	err = conn.PublishRequest(subj, inbox, nil)
	if err == nil {
		m, err = s.NextMsg(timeout)
	}
	s.Unsubscribe()
	return
}
