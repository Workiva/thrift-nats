package thrift_nats

import (
	"bufio"
	"fmt"
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

var ErrTimeout = thrift.NewTTransportExceptionFromError(timeout{})

func main() {
	fmt.Println("nothing happens")
}

type TimeoutReader struct {
	buff    *bufio.Reader
	timeout time.Duration
	ch      <-chan error
}

func NewTimeoutReader(r io.Reader) *TimeoutReader {
	return &TimeoutReader{buff: bufio.NewReaderSize(r, bufferSize), timeout: -1}
}

// SetTimeout sets the timeout for all future Read calls as follows:
//
// 	t < 0  -- block
// 	t == 0 -- poll
// 	t > 0  -- timeout after t
func (r *TimeoutReader) SetTimeout(t time.Duration) time.Duration {
	prev := r.timeout
	r.timeout = t
	return prev
}

func (r *TimeoutReader) Read(b []byte) (n int, err error) {
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
		err = <-r.ch // Block
	} else {
		select {
		case err = <-r.ch: // Poll
		default:
			if r.timeout == 0 {
				return 0, ErrTimeout
			}
			select {
			case err = <-r.ch: // Timeout
			case <-time.After(r.timeout):
				return 0, ErrTimeout
			}
		}
	}
	r.ch = nil
	if r.buff.Buffered() > 0 {
		n, _ = r.buff.Read(b)
	}
	return
}
