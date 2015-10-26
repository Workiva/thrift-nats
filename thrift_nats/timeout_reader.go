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
	"bufio"
	"io"
	"runtime"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type timeout struct{}

func (t timeout) Error() string {
	return "timeout"
}

func (t timeout) Timeout() bool {
	return true
}

var errTimeout = thrift.NewTTransportExceptionFromError(timeout{})

type timeoutReader struct {
	buff    *bufio.Reader
	timeout time.Duration
	ch      <-chan error
}

func newTimeoutReader(r io.Reader) *timeoutReader {
	return &timeoutReader{buff: bufio.NewReader(r), timeout: -1}
}

func (r *timeoutReader) SetTimeout(t time.Duration) time.Duration {
	prev := r.timeout
	r.timeout = t
	return prev
}

func (r *timeoutReader) Read(b []byte) (n int, err error) {
	if r.ch == nil {
		if r.timeout < 0 || r.buff.Buffered() > 0 {
			return r.buff.Read(b)
		}
		ch := make(chan error, 1)
		r.ch = ch
		go func() {
			_, err := r.buff.Peek(1)
			ch <- err
		}()
		runtime.Gosched()
	}
	if r.timeout < 0 {
		err = <-r.ch
	} else {
		select {
		case err = <-r.ch:
		default:
			if r.timeout == 0 {
				return 0, errTimeout
			}
			select {
			case err = <-r.ch:
			case <-time.After(r.timeout):
				return 0, errTimeout
			}
		}
	}
	r.ch = nil
	if r.buff.Buffered() > 0 {
		n, _ = r.buff.Read(b)
	}
	return
}
