package main

import (
	"bytes"
	"io"
	"log"
	"sync"
)

// A Sender turns a chan<- []byte into a thing which can be written to
type Sender chan<- []byte

var _ io.WriteCloser = (*Sender)(nil)

func (s *Sender) Write(data []byte) (n int, err error) {
	*s <- data
	return len(data), nil
}

func (s *Sender) Close() (err error) {
	close(*s)
	return
}

// A Receiver turns a <-chan []byte into a thing which can be read from
type Receiver struct {
	c <-chan []byte
	b bytes.Buffer
}

var _ io.Reader = (*Receiver)(nil)

func NewReceiver(c <-chan []byte) *Receiver {
	return &Receiver{c, bytes.Buffer{}}
}

func (r *Receiver) Read(readbuf []byte) (n int, err error) {
	data, ok := <-r.c
	if !ok {
		return 0, io.EOF
	}
	n, err = r.b.Write(data)
	if err != nil || n < len(data) {
		return
	}
	return r.b.Read(readbuf)
}

// Send bytes from `reader` onto `sender` and when `reader` closes, close sender.
func SendForReader(sender chan<- []byte, reader io.ReadCloser) {
	s := Sender(sender)
	defer s.Close()

	_, err := io.Copy(&s, reader)
	if err != nil {
		log.Panicf("SendForReader panicked! %v", err)
	}
}

// Send bytes from `receiver` onto `writer` and close `writer` when `receiver` closes.
// the `wg` WaitGroup is notified when we close
func RecieveForWriter(wg *sync.WaitGroup, receiver <-chan []byte, writer io.WriteCloser) {
	r := NewReceiver(receiver)
	defer wg.Done()
	defer writer.Close()

	_, err := io.Copy(writer, r)
	if err != nil {
		log.Panicf("ReceiveForWriter panicked! %v", err)
	}
}
