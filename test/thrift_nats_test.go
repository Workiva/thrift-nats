package test

import (
	"fmt"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"

	"github.com/Workiva/thrift-nats/test/gen-go/tutorial"
	"github.com/Workiva/thrift-nats/thrift_nats"
)

const (
	port    = 9972
	subject = "test"
)

func TestRPC(t *testing.T) {
	options := &DefaultTestOptions
	options.Port = port
	server := RunServer(options)
	if server == nil {
		t.Fatal("Failed to start gnatsd")
	}
	defer server.Shutdown()

	testRPCHappyPath(t, thrift.NewTJSONProtocolFactory())
	testRPCHappyPath(t, thrift.NewTBinaryProtocolFactoryDefault())
	testRPCHappyPath(t, thrift.NewTCompactProtocolFactory())
	testRPCNoServer(t)
}

func testRPCHappyPath(t *testing.T, proto thrift.TProtocolFactory) {
	testHappyPath(t, proto, true, false)
	testHappyPath(t, proto, true, true)
	testHappyPath(t, proto, false, true)
	testHappyPath(t, proto, false, false)
}

func testHappyPath(t *testing.T, proto thrift.TProtocolFactory, buffered, framed bool) {
	options := nats.DefaultOptions
	options.Servers = []string{fmt.Sprintf("nats://localhost:%d", port)}
	serverConn, err := options.Connect()
	if err != nil {
		t.Fatal("Failed to connect to gnatsd", err)
	}
	defer serverConn.Close()
	clientConn, err := options.Connect()
	if err != nil {
		t.Fatal("Failed to connect to gnatsd", err)
	}
	defer clientConn.Close()

	var transportFactory thrift.TTransportFactory
	if buffered {
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	} else {
		transportFactory = thrift.NewTTransportFactory()
	}

	if framed {
		transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	}

	processor := tutorial.NewCalculatorProcessor(NewCalculatorHandler())
	server := thrift_nats.NewNATSServer(serverConn, subject, -1, -1, processor,
		transportFactory, proto)

	wait := make(chan bool)
	go func() {
		wait <- true
		if err := server.Serve(); err != nil {
			t.Fatal("Failed to start thrift server", err)
		}
	}()
	<-wait
	defer server.Stop()

	// Give the server a chance to start.
	time.Sleep(500 * time.Millisecond)

	transport, err := thrift_nats.NATSTransportFactory(clientConn, subject, time.Second, -1)
	if err != nil {
		t.Fatal("Failed to create NATS transport", err)
	}
	transport = transportFactory.GetTransport(transport)
	if err := transport.Open(); err != nil {
		t.Fatal("Failed to open transport", err)
	}
	defer transport.Close()

	client := tutorial.NewCalculatorClientFactory(transport, proto)
	if err := client.Ping(); err != nil {
		t.Fatal("Ping failed", err)
	}
	if err := client.Zip(); err != nil {
		t.Fatal("Zip failed", err)
	}
	sum, err := client.Add(1, 1)
	if err != nil {
		t.Fatal("Sum failed", err)
	}
	if sum != 2 {
		t.Fatalf("Expected Sum result 2, got %d", sum)
	}

	work := tutorial.NewWork()
	work.Op = tutorial.Operation_DIVIDE
	work.Num1 = 8
	work.Num2 = 4
	quotient, err := client.Calculate(1, work)
	if err != nil {
		t.Fatal("Calculate failed", err)
	}
	if sum != 2 {
		t.Fatalf("Expected Calculate result 4, got %d", quotient)
	}

	work.Op = tutorial.Operation_DIVIDE
	work.Num1 = 1
	work.Num2 = 0
	_, err = client.Calculate(2, work)
	switch v := err.(type) {
	case *tutorial.InvalidOperation:
		break
	default:
		t.Fatalf("Expected Calculate to return InvalidOperation, got %s", v)
	}

	log, err := client.GetStruct(1)
	if err != nil {
		t.Fatal("GetStruct failed", err)
	}
	if log.Value != "2" {
		t.Fatalf("Expected struct value `2`, got `%s`", log.Value)
	}

	log, err = client.GetStruct(2)
	if err != nil {
		t.Fatal("GetStruct failed", err)
	}
	if log != nil {
		t.Fatalf("Expected nil struct, got `%s`", log)
	}
}

func testRPCNoServer(t *testing.T) {
	options := nats.DefaultOptions
	options.Servers = []string{fmt.Sprintf("nats://localhost:%d", port)}
	clientConn, err := options.Connect()
	if err != nil {
		t.Fatal("Failed to connect to gnatsd", err)
	}
	defer clientConn.Close()
	_, err = thrift_nats.NATSTransportFactory(clientConn, subject, 200*time.Millisecond, -1)
	if err == nil {
		t.Fatal("Expected timeout error")
	}
}
