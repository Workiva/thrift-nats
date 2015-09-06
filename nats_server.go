package thrift_nats

import (
	"log"
	"runtime/debug"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

type TNATSServer struct {
	conn                   *nats.Conn
	subject                string
	quit                   chan struct{}
	processorFactory       thrift.TProcessorFactory
	serverTransport        *TServerNATS
	inputTransportFactory  thrift.TTransportFactory
	outputTransportFactory thrift.TTransportFactory
	inputProtocolFactory   thrift.TProtocolFactory
	outputProtocolFactory  thrift.TProtocolFactory
}

func NewTNATSServer6(
	conn *nats.Conn,
	subject string,
	processor thrift.TProcessor,
	serverTransport *TServerNATS,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) *TNATSServer {

	return NewTNATSServerFactory6(
		conn,
		subject,
		thrift.NewTProcessorFactory(processor),
		serverTransport,
		transportFactory,
		protocolFactory,
	)
}

func NewTNATSServerFactory6(
	conn *nats.Conn,
	subject string,
	processorFactory thrift.TProcessorFactory,
	serverTransport *TServerNATS,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) *TNATSServer {

	return NewTNATSServerFactory8(
		conn,
		subject,
		processorFactory,
		serverTransport,
		transportFactory,
		transportFactory,
		protocolFactory,
		protocolFactory,
	)
}

func NewTNATSServerFactory8(
	conn *nats.Conn,
	subject string,
	processorFactory thrift.TProcessorFactory,
	serverTransport *TServerNATS,
	inputTransportFactory thrift.TTransportFactory,
	outputTransportFactory thrift.TTransportFactory,
	inputProtocolFactory thrift.TProtocolFactory,
	outputProtocolFactory thrift.TProtocolFactory) *TNATSServer {

	return &TNATSServer{
		conn:                   conn,
		subject:                subject,
		processorFactory:       processorFactory,
		serverTransport:        serverTransport,
		inputTransportFactory:  inputTransportFactory,
		outputTransportFactory: outputTransportFactory,
		inputProtocolFactory:   inputProtocolFactory,
		outputProtocolFactory:  outputProtocolFactory,
		quit: make(chan struct{}, 1),
	}
}

func (p *TNATSServer) ProcessorFactory() thrift.TProcessorFactory {
	return p.processorFactory
}

func (p *TNATSServer) ServerTransport() thrift.TServerTransport {
	return p.serverTransport
}

func (p *TNATSServer) InputTransportFactory() thrift.TTransportFactory {
	return p.inputTransportFactory
}

func (p *TNATSServer) OutputTransportFactory() thrift.TTransportFactory {
	return p.outputTransportFactory
}

func (p *TNATSServer) InputProtocolFactory() thrift.TProtocolFactory {
	return p.inputProtocolFactory
}

func (p *TNATSServer) OutputProtocolFactory() thrift.TProtocolFactory {
	return p.outputProtocolFactory
}

func (p *TNATSServer) Listen() error {
	return p.serverTransport.Listen()
}

func (p *TNATSServer) AcceptLoop() error {
	sub, err := p.conn.Subscribe(p.subject, func(msg *nats.Msg) {
		if msg.Reply != "" {
			listenTo := nats.NewInbox()
			if err := p.conn.PublishRequest(msg.Reply, listenTo, nil); err == nil {
				p.accept(listenTo, msg.Reply)
			}
		}
	})
	if err != nil {
		return err
	}

	<-p.quit
	return sub.Unsubscribe()
}

func (p *TNATSServer) accept(listenTo, replyTo string) {
	client := p.serverTransport.AcceptNATS(listenTo, replyTo)
	if err := client.Open(); err != nil {
		log.Println("error opening client transport:", err)
	}
	if err := p.processRequests(client); err != nil {
		log.Println("error processing request:", err)
	}
}

func (p *TNATSServer) Serve() error {
	if err := p.Listen(); err != nil {
		return err
	}
	p.AcceptLoop()
	return nil
}

func (p *TNATSServer) Stop() error {
	p.quit <- struct{}{}
	p.serverTransport.Interrupt()
	return nil
}

func (p *TNATSServer) processRequests(client thrift.TTransport) error {
	processor := p.processorFactory.GetProcessor(client)
	inputTransport := p.inputTransportFactory.GetTransport(client)
	outputTransport := p.outputTransportFactory.GetTransport(client)
	inputProtocol := p.inputProtocolFactory.GetProtocol(inputTransport)
	outputProtocol := p.outputProtocolFactory.GetProtocol(outputTransport)
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
			log.Printf("error processing request: %s", err)
			return err
		}
		if !ok {
			break
		}
	}
	return nil
}
