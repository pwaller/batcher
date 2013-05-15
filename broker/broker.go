package broker

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"

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

func HandleWorker(send, recv func(interface{}) error) error {
	send("ok")
	return nil
}

func HandleJob(send, recv func(interface{}) error) error {
	err := send("ok")
	if err != nil {
		return err
	}

	var args []string
	err = recv(&args)
	if err != nil {
		return err
	}

	var jobinfo util.JobInfo
	err = recv(&jobinfo)
	if err != nil {
		return err
	}

	log.Printf("Got arguments to run: %v", args)

	cmd := exec.Command(args[0], args[1:]...)

	var stdin io.WriteCloser

	if jobinfo.HasTerm {
		pipe, child, err := pty.Open()
		if err != nil {
			panic(err)
		}
		cmd.Stdin = child
		cmd.SysProcAttr = &syscall.SysProcAttr{Setctty: true, Setsid: true}
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

	err = cmd.Start()
	if err != nil {
		log.Print("Process failed")
		send(util.Message{Code: util.END, Err: err})
		return err
	}

	// Process stdin
	go func() {
		log.Printf("Processing stdin")
		defer util.HandleNetClose()

		var m util.Message
		for {
			err := recv(&m)
			if err != nil {
				panic(err)
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
				return

			case util.SIGINT:
				cmd.Process.Signal(os.Interrupt)

			default:
				panic(fmt.Errorf("Unhandled code: %v", m.Code))
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
			log.Printf("%#+v %v %v", status, status.Signaled(), status.Signal())
			if status.Signaled() {
				send(util.Message{Code: util.END, Signal: status.Signal()})
				return nil
			}
			if status.Exited() {
				send(util.Message{Code: util.END, Reason: status.ExitStatus()})
				return nil
			}
			log.Panicf("Process hasn't exited when it should have.. %#+v", err.ProcessState)

			return err
		default:
			panic(err)
		}
	}
	send(util.Message{Code: util.END})
	return nil
}

func ServeOne(conn net.Conn) {
	defer conn.Close()

	defer func() {
		return
		if err := recover(); err != nil {
			// log.Printf("Panicked whilst server connection")
			// TODO(pwaller): something more graceful
			return
		}
	}()

	send, recv := util.Gobber(conn)

	var client_type string
	err := recv(&client_type)
	if err != nil {
		panic(err)
	}

	log.Printf("Got a new connection: %q", client_type)

	switch client_type {
	case "worker":
		err := HandleWorker(send, recv)
		if err != nil {
			panic(err)
		}
	case "job":
		err := HandleJob(send, recv)
		log.Print("Job handled: ", err)
		if err != nil {
			panic(err)
		}
	default:
	}

}

func Serve(l net.Listener) error {
	defer l.Close()
	var tempDelay time.Duration // how long to sleep on accept failure

	log.Print("Serving connections.. aasdf")

	for {
		client, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Printf("Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0

		address := client.RemoteAddr().(*net.TCPAddr)
		if !address.IP.IsLoopback() {
			log.Printf("Rejecting non-loopback connection!")
			client.Close()
			continue
		}

		if !util.CheckUser(address) {
			log.Printf("Rejecting connection from different user!")
			client.Close()
			continue
		}

		go ServeOne(client)
	}
}

func Broker() {
	l, e := net.Listen("tcp", "localhost:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	Serve(l)
}
