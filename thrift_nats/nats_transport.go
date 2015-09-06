package thrift_nats

import (
	"io"
	"log"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

type natsTransport struct {
	conn              *nats.Conn
	listenTo          string
	replyTo           string
	heartbeat         string
	heartbeatInterval time.Duration
	sub               *nats.Subscription
	closed            chan struct{}
	reader            *TimeoutReader
	writer            *io.PipeWriter
}

// NewNATSTransport returns a Thrift TTransport which uses the NATS messaging
// system as the underlying transport. This TTransport can only be used with
// NATSServer.
func NewNATSTransport(conn *nats.Conn, listenTo, replyTo, heartbeat string,
	heartbeatInterval, readTimeout time.Duration) thrift.TTransport {

	reader, writer := io.Pipe()
	timeoutReader := NewTimeoutReader(reader)
	timeoutReader.SetTimeout(readTimeout)
	return &natsTransport{
		conn:              conn,
		listenTo:          listenTo,
		replyTo:           replyTo,
		heartbeat:         heartbeat,
		heartbeatInterval: heartbeatInterval,
		closed:            make(chan struct{}),
		reader:            timeoutReader,
		writer:            writer,
	}
}

func (t *natsTransport) Open() error {
	sub, err := t.conn.Subscribe(t.listenTo, func(msg *nats.Msg) {
		t.writer.Write(msg.Data)
	})
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	t.sub = sub
	if t.heartbeatInterval > 0 {
		go t.startHeartbeat()
	}
	return nil
}

func (t *natsTransport) IsOpen() bool {
	return t.sub != nil
}

func (t *natsTransport) Close() error {
	if !t.IsOpen() {
		return nil
	}
	if err := t.sub.Unsubscribe(); err != nil {
		return err
	}
	if t.heartbeatInterval > 0 {
		t.closed <- struct{}{}
	}
	t.sub = nil
	return nil
}

func (t *natsTransport) Read(p []byte) (int, error) {
	return t.reader.Read(p)
}

func (t *natsTransport) Write(p []byte) (int, error) {
	if err := t.conn.Publish(t.replyTo, p); err != nil {
		return 0, thrift.NewTTransportExceptionFromError(err)
	}
	return len(p), nil
}

func (t *natsTransport) Flush() error {
	return nil
}

func (t *natsTransport) RemainingBytes() uint64 {
	return 0
}

func (t *natsTransport) startHeartbeat() {
	for {
		select {
		case <-time.After(t.heartbeatInterval):
			if err := t.conn.Publish(t.heartbeat, nil); err != nil {
				log.Println("thrift_nats: error sending heartbeat", err)
			}
		case <-t.closed:
			return
		}
	}
}
