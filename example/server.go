package main

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import (
	"fmt"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/nats-io/nats"
	"github.com/tylertreat/thrift-nats/example/gen-go/tutorial"
	"github.com/tylertreat/thrift-nats/thrift_nats"
)

func runServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	var err error
	opts := nats.DefaultOptions
	opts.Servers = []string{addr}
	if secure {
		opts.Secure = true
	}
	conn, err := opts.Connect()
	if err != nil {
		return err
	}

	handler := NewCalculatorHandler()
	processor := tutorial.NewCalculatorProcessor(handler)
	server := thrift_nats.NewNATSServer6(conn, "foo", 3*time.Second, processor,
		transportFactory, protocolFactory)

	fmt.Println("Starting the NATS server connecting to", addr)
	return server.Serve()
}
