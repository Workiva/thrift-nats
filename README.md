# thrift-nats

Support for using [NATS](http://nats.io/) as a [Thrift](https://thrift.apache.org/) RPC transport.

## Server Usage

```go
options := nats.DefaultOptions
conn, err := options.Connect()
if err != nil {
    panic(err)
}

var (
    transportFactory = thrift.NewTBufferedTransportFactory(8192)
    protocolFactory  = thrift.NewTJSONProtocolFactory()
    server           = thrift_nats.NewNATSServer(conn, "my-service", -1, 5*time.Second,
        processor, transportFactory, protocolFactory)
)

if err := server.Serve(); err != nil {
    panic(err)
}
```

## Client Usage

```go
options := nats.DefaultOptions
conn, err := options.Connect()
if err != nil {
    panic(err)
}

var (
    transportFactory = thrift.NewTBufferedTransportFactory(8192)
    protocolFactory  = thrift.NewTJSONProtocolFactory()
)

transport, err := thrift_nats.NATSTransportFactory(conn, "my-service", time.Second, time.Second)
if err != nil {
    panic(err)
}
transport = transportFactory.GetTransport(transport)

defer transport.Close()
if err := transport.Open(); err != nil {
    panic(err)
}

handleClient(tutorial.NewCalculatorClientFactory(transport, protocolFactory))
```

## How It Works

A thrift-nats server has a specified NATS subject which it listens on. A client publishes a handshake message on this subject containing the inbox it expects responses to requests on. The handshake is delivered round-robin to a server which then responds and provides an inbox for the client to send requests on and, optionally, a heartbeat deadline. The server then begins accepting requests on the inbox, and the client can begin sending requests. If a heartbeat deadline is specified, the client periodically emits a heartbeat telling the server that it's still alive within the specified interval. If the client misses more than three heartbeats, the server closes the client's session and attempts to signal the client of the disconnect. When the client transport is closed, it attempts to signal the server so the session can be closed. The heartbeat acts as a fail-safe in the event that the disconnect signal is missed by the server.
