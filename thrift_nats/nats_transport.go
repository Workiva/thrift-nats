package thrift_nats

import (
	"io"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

const disconnect = "DISCONNECT"

type natsTransport struct {
	conn     *nats.Conn
	listenTo string
	replyTo  string
	sub      *nats.Subscription
	reader   *timeoutReader
	writer   *io.PipeWriter
}

// NewNATSTransport returns a Thrift TTransport which uses the NATS messaging
// system as the underlying transport. This TTransport can only be used with
// NATSServer.
func NewNATSTransport(conn *nats.Conn, listenTo, replyTo string,
	readTimeout time.Duration) thrift.TTransport {

	reader, writer := io.Pipe()
	timeoutReader := newTimeoutReader(reader)
	timeoutReader.SetTimeout(readTimeout)
	return &natsTransport{
		conn:     conn,
		listenTo: listenTo,
		replyTo:  replyTo,
		reader:   timeoutReader,
		writer:   writer,
	}
}

func (t *natsTransport) Open() error {
	sub, err := t.conn.Subscribe(t.listenTo, func(msg *nats.Msg) {
		if msg.Reply == disconnect {
			// Remote client is disconnecting.
			t.writer.Close()
			return
		}
		t.writer.Write(msg.Data)
	})
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	t.conn.Flush() // Ensure subscription is processed before moving on.
	t.sub = sub
	return nil
}

func (t *natsTransport) IsOpen() bool {
	return t.sub != nil
}

func (t *natsTransport) Close() error {
	if !t.IsOpen() {
		return nil
	}
	// Signal remote peer for a graceful disconnect.
	t.conn.PublishRequest(t.replyTo, disconnect, nil)
	if err := t.sub.Unsubscribe(); err != nil {
		return err
	}
	t.conn.Flush()
	t.sub = nil
	return nil
}

func (t *natsTransport) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	return n, thrift.NewTTransportExceptionFromError(err)
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
