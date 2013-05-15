package worker

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"

	"github.com/kr/pty"
	"github.com/pwaller/batcher/util"
)

type Sender struct {
	code util.Code
	send func(interface{}) error
}

func (s *Sender) Write(data []byte) (int, error) {
	log.Printf("Data: %q %q", s.code, string(data))
	err := s.send(util.Message{Code: s.code, Content: data})
	return len(data), err
}

func HandleJob(jobinfo util.JobInfo, send, recv func(interface{}) error) error {
	log.Printf("Got arguments to run: %v", jobinfo.Args)

	cmd := exec.Command(jobinfo.Args[0], jobinfo.Args[1:]...)

	var stdin io.WriteCloser

	if jobinfo.HasTerm {
		pipe, child, err := pty.Open()
		if err != nil {
			panic(err)
		}
		defer pipe.Close()
		defer child.Close()
		cmd.Stdin = child
		stdin = pipe
	} else {
		pipe, err := cmd.StdinPipe()
		if err != nil {
			panic(err)
		}
		stdin = pipe
	}

	// Known issue with this technique:
	// Take the python prompt.
	//   >>> print "Hello world"
	//   >>> Hello world
	// ... oops, what happened? The STDERR write of ">>> " got reordered WRT
	// the STDOUT write of "Hello world". The program may issue the write
	// syscalls in a fixed order, but select'ing against the two pipes might
	// yield a different order. No clue how to solve this.
	//
	// One possible way around might be to ensure that the FD's flush on every
	// syscall.
	//
	// Suggestion: write testcase which is able to reproduce it with enough
	// iterations
	cmd.Stdout = &Sender{util.STDOUT, send}
	cmd.Stderr = &Sender{util.STDERR, send}

	err := cmd.Start()
	if err != nil {
		log.Printf("Process failed %#+v %s", err, err)
		err := send(util.Message{Code: util.END, Err: err})
		if err != nil {
			panic(err)
		}
		return err
	}

	stdin_recv_finished := make(chan bool, 1)
	defer func() {
		if err := recover(); err != nil {
			// Don't wait for the signal if we're panicking.
			panic(err)
		}
		// Block function exit until recv message processing is done
		// (to maintain state)
		<-stdin_recv_finished
	}()

	// This goroutine hijacks `recv` until it sees an END with the above defer
	// waiting on <-stdin_recv_finished
	go func() {
		defer func() { stdin_recv_finished <- true }()

		log.Printf("Processing stdin")

		var m util.Message
		for {
			// Pump messages from the (client->Broker)-> to the process
			err := recv(&m)
			if err != nil {
				// Broker connection ended
				log.Panicf("Broker connection ended receving messages: %q", err)
			}

			switch m.Code {
			case util.STDIN:
				// Here is something to feed to stdin..
				for len(m.Content) > 0 {
					n, err := stdin.Write(m.Content)
					m.Content = m.Content[n:]
					if err != nil {
						panic(err)
					}
				}

			case util.STDIN_CLOSED:
				cmd.Process.Signal(syscall.SIGHUP)

			case util.SIGINT:
				cmd.Process.Signal(os.Interrupt)

			case util.END:
				return

			default:
				panic(fmt.Errorf("Unhandled code in worker comm loop: %v", m.Code))
			}
		}
	}()

	err = cmd.Wait()
	log.Print("Finished waiting for process..")

	if err != nil {
		switch err := err.(type) {
		case *exec.ExitError:
			// Unix only
			status := err.ProcessState.Sys().(syscall.WaitStatus)
			if status.Signaled() {
				err := send(util.Message{Code: util.END, Signal: status.Signal()})
				if err != nil {
					log.Panic(err)
				}
				return nil
			}
			if status.Exited() {
				err := send(util.Message{Code: util.END, Reason: status.ExitStatus()})
				if err != nil {
					panic(err)
				}
				return nil
			}
			log.Panicf("Process hasn't exited when it should have.. %#+v", err.ProcessState)
			return err
		default:
			panic(err)
		}
	}

	err = send(util.Message{Code: util.END})
	if err != nil {
		return err
	}
	return nil
}

func Worker(send, recv func(interface{}) error) error {
	err := send("worker")
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

	var m util.Message
	for {
		err := recv(&m)
		if err == io.EOF {
			log.Print("Broker closed connection")
			return nil
		}
		if err != nil {
			return err
		}

		switch m.Code {
		case util.WORKER_NEWJOB:

			log.Printf("Handling job..")
			err = HandleJob(m.Job, send, recv)
			log.Printf(" .. job finished. %v", err)
			// TODO(pwaller): We should be robust against all types of failure
			// other than socket failure and end of the whole universe here.
			if err != nil {
				return err
			}

		default:
			log.Panicf("Recieved unexpected code %v", m.Code)
		}
	}

	return nil
}
