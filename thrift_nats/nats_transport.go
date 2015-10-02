package thrift_nats

import (
	"bytes"
	"io"
	"log"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

const (
	disconnect     = "DISCONNECT"
	maxMessageSize = 1024 * 1024
)

type natsTransport struct {
	conn              *nats.Conn
	listenTo          string
	replyTo           string
	sub               *nats.Subscription
	reader            *timeoutReader
	writer            *io.PipeWriter
	heartbeat         string
	heartbeatInterval time.Duration
	closed            chan struct{}
	writeBuffer       *bytes.Buffer
}

// NewNATSTransport returns a Thrift TTransport which uses the NATS messaging
// system as the underlying transport. This TTransport can only be used with
// NATSServer.
func NewNATSTransport(conn *nats.Conn, listenTo, replyTo, heartbeat string,
	readTimeout, heartbeatInterval time.Duration) thrift.TTransport {

	reader, writer := io.Pipe()
	timeoutReader := newTimeoutReader(reader)
	timeoutReader.SetTimeout(readTimeout)
	buf := make([]byte, 0, maxMessageSize)
	return &natsTransport{
		conn:              conn,
		listenTo:          listenTo,
		replyTo:           replyTo,
		reader:            timeoutReader,
		writer:            writer,
		heartbeat:         heartbeat,
		heartbeatInterval: heartbeatInterval,
		closed:            make(chan struct{}),
		writeBuffer:       bytes.NewBuffer(buf),
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
	remaining := t.writeBuffer.Cap() - t.writeBuffer.Len()
	if remaining < len(p) {
		t.writeBuffer.Write(p[0:remaining])
		if err := t.Flush(); err != nil {
			return 0, thrift.NewTTransportExceptionFromError(err)
		}
		return t.Write(p[remaining:])
	}
	return t.writeBuffer.Write(p)
}

func (t *natsTransport) Flush() error {
	data := t.writeBuffer.Bytes()
	err := t.conn.Publish(t.replyTo, data)
	t.writeBuffer.Reset()
	return thrift.NewTTransportExceptionFromError(err)
}

func (t *natsTransport) RemainingBytes() uint64 {
	return ^uint64(0) // We just don't know unless framed is used.
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
