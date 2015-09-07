package thrift_nats

import (
	"bufio"
	"io"
	"runtime"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

const bufferSize = 4096

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
	return &timeoutReader{buff: bufio.NewReaderSize(r, bufferSize), timeout: -1}
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
