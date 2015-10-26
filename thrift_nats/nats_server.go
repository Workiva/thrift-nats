/*
Copyright 2015 Workiva

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package thrift_nats

import (
	"log"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
)

const (
	queue               = "rpc"
	maxMissedHeartbeats = 3
)

type natsServer struct {
	conn                   *nats.Conn
	subject                string
	clientTimeout          time.Duration
	heartbeatDeadline      time.Duration
	clients                map[string]thrift.TTransport
	mu                     sync.Mutex
	closed                 bool
	quit                   chan struct{}
	processorFactory       thrift.TProcessorFactory
	serverTransport        *natsServerTransport
	inputTransportFactory  thrift.TTransportFactory
	outputTransportFactory thrift.TTransportFactory
	inputProtocolFactory   thrift.TProtocolFactory
	outputProtocolFactory  thrift.TProtocolFactory
}

// NewNATSServer returns a Thrift TServer which uses the NATS messaging system
// as the underlying transport. The subject is the NATS subject used for
// connection handshakes. The client timeout controls the read timeout on the
// client connection (negative value for no timeout). The heartbeat deadline
// controls how long clients have to respond with a heartbeat (negative value
// for no heartbeats).
func NewNATSServer(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	heartbeatDeadline time.Duration,
	processor thrift.TProcessor,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {

	return NewNATSServerFactory7(
		conn,
		subject,
		clientTimeout,
		heartbeatDeadline,
		thrift.NewTProcessorFactory(processor),
		transportFactory,
		protocolFactory,
	)
}

func NewNATSServerFactory7(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	heartbeatDeadline time.Duration,
	processorFactory thrift.TProcessorFactory,
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory) thrift.TServer {

	return NewNATSServerFactory9(
		conn,
		subject,
		clientTimeout,
		heartbeatDeadline,
		processorFactory,
		transportFactory,
		transportFactory,
		protocolFactory,
		protocolFactory,
	)
}

func NewNATSServerFactory9(
	conn *nats.Conn,
	subject string,
	clientTimeout time.Duration,
	heartbeatDeadline time.Duration,
	processorFactory thrift.TProcessorFactory,
	inputTransportFactory thrift.TTransportFactory,
	outputTransportFactory thrift.TTransportFactory,
	inputProtocolFactory thrift.TProtocolFactory,
	outputProtocolFactory thrift.TProtocolFactory) thrift.TServer {

	return &natsServer{
		conn:                   conn,
		subject:                subject,
		clientTimeout:          clientTimeout,
		heartbeatDeadline:      heartbeatDeadline,
		clients:                make(map[string]thrift.TTransport),
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
			var (
				heartbeat   = nats.NewInbox()
				listenTo    = nats.NewInbox()
				client, err = n.accept(listenTo, msg.Reply, heartbeat)
			)
			if err != nil {
				log.Println("thrift_nats: error accepting client transport:", err)
				return
			}

			if n.isHeartbeating() {
				n.mu.Lock()
				n.clients[heartbeat] = client
				n.mu.Unlock()
			}

			connectMsg := heartbeat + " " + strconv.FormatInt(int64(n.heartbeatDeadline.Seconds())*1000, 10)
			if err := n.conn.PublishRequest(msg.Reply, listenTo, []byte(connectMsg)); err != nil {
				log.Println("thrift_nats: error publishing transport inbox:", err)
				if n.isHeartbeating() {
					n.remove(heartbeat)
				}
			} else if n.isHeartbeating() {
				go n.acceptHeartbeat(heartbeat)
			}
		} else {
			log.Printf("thrift_nats: discarding invalid connect message %+v", msg)
		}
	})
	if err != nil {
		return err
	}

	n.conn.Flush()
	n.mu.Lock()
	n.closed = false
	n.mu.Unlock()

	log.Println("thrift_nats: server running...")
	<-n.quit
	return sub.Unsubscribe()
}

func (n *natsServer) remove(heartbeat string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	client, ok := n.clients[heartbeat]
	if !ok {
		return
	}
	client.Close()
	delete(n.clients, heartbeat)
}

func (n *natsServer) acceptHeartbeat(heartbeat string) {
	missed := 0
	recvHeartbeat := make(chan struct{})

	sub, err := n.conn.Subscribe(heartbeat, func(msg *nats.Msg) {
		recvHeartbeat <- struct{}{}
	})
	if err != nil {
		log.Println("thrift_nats: error subscribing to heartbeat", heartbeat)
		return
	}

	for {
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		select {
		case <-time.After(time.Duration(n.heartbeatDeadline)):
			missed += 1
			if missed >= maxMissedHeartbeats {
				log.Println("thrift_nats: client heartbeat expired")
				n.remove(heartbeat)
				sub.Unsubscribe()
				return
			}
		case <-recvHeartbeat:
			missed = 0
		}
	}
}

func (n *natsServer) accept(listenTo, replyTo, heartbeat string) (thrift.TTransport, error) {
	client := n.serverTransport.AcceptNATS(listenTo, replyTo, n.clientTimeout)
	if err := client.Open(); err != nil {
		return nil, err
	}
	go func() {
		if err := n.processRequests(client); err != nil {
			log.Println("thrift_nats: error processing request:", err)
		}
		n.remove(heartbeat)
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
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
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

func (n *natsServer) isHeartbeating() bool {
	return n.heartbeatDeadline > 0
}
