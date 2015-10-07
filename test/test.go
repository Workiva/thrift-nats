package test

import (
	"fmt"
	"net"
	"time"

	"github.com/nats-io/gnatsd/server"
)

var DefaultTestOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoLog:  true,
	NoSigs: true,
}

func RunDefaultServer() *server.Server {
	return RunServer(&DefaultTestOptions)
}

// New Go Routine based server
func RunServer(opts *server.Options) *server.Server {
	return RunServerWithAuth(opts, nil)
}

func RunServerWithConfig(configFile string) (srv *server.Server, opts *server.Options) {
	opts, err := server.ProcessConfigFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Error processing configuration file: %v", err))
	}
	opts.NoSigs, opts.NoLog = true, true
	srv = RunServer(opts)
	return
}

// New Go Routine based server with auth
func RunServerWithAuth(opts *server.Options, auth server.Auth) *server.Server {
	if opts == nil {
		opts = &DefaultTestOptions
	}
	s := server.New(opts)
	if s == nil {
		panic("No NATS Server object returned.")
	}

	if auth != nil {
		s.SetAuthMethod(auth)
	}

	// Run server in Go routine.
	go s.Start()

	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		addr := s.Addr()
		if addr == nil {
			time.Sleep(50 * time.Millisecond)
			// Retry. We might take a little while to open a connection.
			continue
		}
		conn, err := net.Dial("tcp", addr.String())
		if err != nil {
			// Retry after 50ms
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		return s
	}
	panic("Unable to start NATS Server in Go Routine")
}
