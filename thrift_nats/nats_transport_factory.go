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
	var interval time.Duration
	if deadline > 0 {
		interval = time.Duration(deadline - (deadline / 4))
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
	s.Unsubscribe()
	return msg, inbox, nil
}
