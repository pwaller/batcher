package client

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"reflect"
	"time"

	"github.com/kless/terminal"

	"github.com/pwaller/batcher/util"
)

var _ = log.Ldate

func AutoPanic(f interface{}) func(...interface{}) {
	v := reflect.ValueOf(f)
	return func(args ...interface{}) {
		var result []reflect.Value
		vargs := make([]reflect.Value, len(args))
		for i, a := range args {
			vargs[i] = reflect.ValueOf(a)
		}

		result = v.Call(vargs)
		if len(result) != 1 {
			panic("This function only works for functions returning one value")
		}
		err := result[0].Interface()
		if err != nil {
			panic(err)
		}
	}
}

// Client side of creating new job
func NewJob(send, recv func(interface{}) error) error {
	err := send("job")
	if err != nil {
		return err
	}
	isterm := terminal.IsTerminal(int(os.Stdin.Fd()))
	err = send(util.JobInfo{isterm, flag.Args()})
	if err != nil {
		return err
	}

	var response string

	err = recv(&response)
	if err != nil {
		return err
	}
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}

	done := make(chan bool)

	// Handle stdin
	go func() {
		defer func() {
			err := send(util.Message{Code: util.STDIN_CLOSED})
			if err != nil && err.Error() != "use of closed network connection" {
				panic(err)
			}
			println() // Mimic terminal behaviour poorly
			// (when Ctrl-D is pressed, a newline comes from somewhere..)
			// (might not be terminal, might be python, my test program.)
		}()

		var buf [10240]byte
		for {
			n, err := os.Stdin.Read(buf[:])
			//log.Printf("Read %v bytes from stdin", n)
			if err != nil {
				return
			}
			err = send(util.Message{Code: util.STDIN, Content: buf[:n]})
			if err != nil {
				panic(err)
			}
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		// Process events (stdout, stderr, program end)
		for {
			var v util.Message
			err := recv(&v)
			if err == io.EOF {
				//log.Print("Server closed connection: ", v)
				done <- true
				return
			}
			if err != nil {
				panic(err)
			}

			switch v.Code {
			case util.STDOUT:
				os.Stdout.Write(v.Content)
			case util.STDERR:
				os.Stderr.Write(v.Content)
			case util.END:
				done <- true
				return
			}
		}
	}()

	var last_interrupt time.Time

	for {
		select {
		case <-interrupt:
			err = send(util.Message{Code: util.SIGINT})
			if err != nil {
				panic(err)
			}
			if time.Since(last_interrupt) < 250*time.Millisecond {
				// Respond to interrupt if two interrupts are received in short
				// succession
				return nil
			}
			last_interrupt = time.Now()
		case <-done:
			return nil
		}
	}

	return nil
}
