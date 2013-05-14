package client

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/kless/terminal"
	"github.com/pwaller/batcher/util"
)

// Client side of creating new job
func NewJob(send, recv func(interface{})) error {
	send("job")
	send(flag.Args())
	send(util.JobInfo{terminal.IsTerminal(int(os.Stdin.Fd()))})

	var response string
	recv(&response)
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}

	done := make(chan bool)

	// Handle stdin
	go func() {
		defer send(util.Message{Code: util.STDIN_CLOSED})

		var buf [10240]byte
		for {
			n, err := os.Stdin.Read(buf[:])
			//log.Printf("Read %v bytes from stdin", n)
			if err != nil {
				return
			}
			send(util.Message{Code: util.STDIN, Content: buf[:n]})
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		for {
			var v util.Message
			recv(&v)

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
			send(util.Message{Code: util.SIGINT})
			if time.Since(last_interrupt) < 250*time.Millisecond {
				// Respond to interrupt
				return nil
			}
			last_interrupt = time.Now()
		case <-done:
			return nil
		}
	}

	return nil
}
