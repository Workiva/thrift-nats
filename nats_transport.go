package thrift_nats

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

type TNATS struct {
	conn     *nats.Conn
	listenTo string
	replyTo  string
	sub      *nats.Subscription
	buff     chan byte
}

func NewTNATS(conn *nats.Conn, listenTo, replyTo string, bufferSize int) *TNATS {
	return &TNATS{
		conn:     conn,
		listenTo: listenTo,
		replyTo:  replyTo,
		buff:     make(chan byte, bufferSize),
	}
}

func (t *TNATS) Open() error {
	sub, err := t.conn.Subscribe(t.listenTo, func(msg *nats.Msg) {
		for _, b := range msg.Data {
			t.buff <- b
		}
	})
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}
	t.sub = sub
	return nil
}

func (t *TNATS) IsOpen() bool {
	return t.sub != nil
}

func (t *TNATS) Close() error {
	if !t.IsOpen() {
		return nil
	}
	if err := t.sub.Unsubscribe(); err != nil {
		return err
	}
	t.sub = nil
	return nil
}

func (t *TNATS) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i++ {
		p[i] = <-t.buff
	}
	return len(p), nil
}

func (t *TNATS) Write(p []byte) (int, error) {
	if err := t.conn.Publish(t.replyTo, p); err != nil {
		return 0, thrift.NewTTransportExceptionFromError(err)
	}
	return len(p), nil
}

func (t *TNATS) Flush() error {
	return nil
}

func (t *TNATS) RemainingBytes() uint64 {
	return 0
}
