package thrift_nats

import (
	"log"
	"runtime/debug"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

const queue = "rpc"

type natsServer struct {
	conn                   *nats.Conn
	subject                string
	clientTimeout          time.Duration
	quit                   chan struct{}
	processorFactory       thrift.TProcessorFactory
	serverTransport        *natsServerTransport
	inputTransportFactory  thrift.TTransportFactory
	outputTransportFactory thrift.TTransportFactory
	inputProtocolFactory   thrift.TProtocolFactory
	outputProtocolFactory  thrift.TProtocolFactory
}

// NewNATSServer6 returns a Thrift TServer which uses the NATS messaging
// system as the underlying transport.
func NewNATSServer6(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	processor thrift.TProcessor,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {

	return NewNATSServerFactory6(
		conn,
		subject,
		clientTimeout,
		thrift.NewTProcessorFactory(processor),
		transportFactory,
		protocolFactory,
	)
}

func NewNATSServerFactory6(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	processorFactory thrift.TProcessorFactory,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {

	return NewNATSServerFactory8(
		conn,
		subject,
		clientTimeout,
		processorFactory,
		transportFactory,
		transportFactory,
		protocolFactory,
		protocolFactory,
	)
}

func NewNATSServerFactory8(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	processorFactory thrift.TProcessorFactory,
	inputTransportFactory thrift.TTransportFactory,
	outputTransportFactory thrift.TTransportFactory,
	inputProtocolFactory thrift.TProtocolFactory,
	outputProtocolFactory thrift.TProtocolFactory) thrift.TServer {

	return &natsServer{
		conn:                   conn,
		subject:                subject,
		clientTimeout:          clientTimeout,
		processorFactory:       processorFactory,
		serverTransport:        newNATSServerTransport(conn),
		inputTransportFactory:  inputTransportFactory,
		outputTransportFactory: outputTransportFactory,
		inputProtocolFactory:   inputProtocolFactory,
		outputProtocolFactory:  outputProtocolFactory,
		quit: make(chan struct{}, 1),
	}
}

func (n *natsServer) ProcessorFactory() thrift.TProcessorFactory {
	return n.processorFactory
}

func (n *natsServer) ServerTransport() thrift.TServerTransport {
	return n.serverTransport
}

func (n *natsServer) InputTransportFactory() thrift.TTransportFactory {
	return n.inputTransportFactory
}

func (n *natsServer) OutputTransportFactory() thrift.TTransportFactory {
	return n.outputTransportFactory
}

func (n *natsServer) InputProtocolFactory() thrift.TProtocolFactory {
	return n.inputProtocolFactory
}

func (n *natsServer) OutputProtocolFactory() thrift.TProtocolFactory {
	return n.outputProtocolFactory
}

func (n *natsServer) Listen() error {
	return n.serverTransport.Listen()
}

func (n *natsServer) AcceptLoop() error {
	sub, err := n.conn.QueueSubscribe(n.subject, queue, func(msg *nats.Msg) {
		if msg.Reply != "" {
			listenTo := nats.NewInbox()
			client, err := n.accept(listenTo, msg.Reply)
			if err != nil {
				log.Println("thrift_nats: error accepting client transport:", err)
				return
			}

			if err := n.conn.PublishRequest(msg.Reply, listenTo, nil); err != nil {
				log.Println("thrift_nats: error publishing transport inbox:", err)
				client.Close()
			}
		} else {
			log.Printf("thrift_nats: discarding invalid connect message %+v", msg)
		}
	})
	if err != nil {
		return err
	}

	n.conn.Flush()

	log.Println("thrift_nats: server running...")
	<-n.quit
	return sub.Unsubscribe()
}

func (n *natsServer) accept(listenTo, replyTo string) (thrift.TTransport, error) {
	client := n.serverTransport.AcceptNATS(listenTo, replyTo, n.clientTimeout)
	if err := client.Open(); err != nil {
		return nil, err
	}
	go func() {
		if err := n.processRequests(client); err != nil {
			log.Println("thrift_nats: error processing request:", err)
		}
	}()
	return client, nil
}

func (n *natsServer) Serve() error {
	if err := n.Listen(); err != nil {
		return err
	}
	n.AcceptLoop()
	return nil
}

func (n *natsServer) Stop() error {
	n.quit <- struct{}{}
	n.serverTransport.Interrupt()
	return nil
}

func (n *natsServer) processRequests(client thrift.TTransport) error {
	processor := n.processorFactory.GetProcessor(client)
	inputTransport := n.inputTransportFactory.GetTransport(client)
	outputTransport := n.outputTransportFactory.GetTransport(client)
	inputProtocol := n.inputProtocolFactory.GetProtocol(inputTransport)
	outputProtocol := n.outputProtocolFactory.GetProtocol(outputTransport)
	defer func() {
		if e := recover(); e != nil {
			log.Printf("panic in processor: %s: %s", e, debug.Stack())
		}
	}()
	if inputTransport != nil {
		defer inputTransport.Close()
	}
	if outputTransport != nil {
		defer outputTransport.Close()
	}
	for {
		ok, err := processor.Process(inputProtocol, outputProtocol)
		if err, ok := err.(thrift.TTransportException); ok && err.TypeId() == thrift.END_OF_FILE {
			return nil
		} else if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	return nil
}
