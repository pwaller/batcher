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
	send func(interface{})
}

func (s *Sender) Write(data []byte) (int, error) {
	log.Printf("Data: %q", string(data))
	s.send(util.Message{Code: s.code, Content: data})
	return len(data), nil
}

func HandleWorker(send, recv func(interface{})) {
	send("ok")
}

func HandleJob(send, recv func(interface{})) {
	send("ok")

	var args []string
	recv(&args)

	var jobinfo util.JobInfo
	recv(&jobinfo)

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

	cmd.Stdout = &Sender{util.STDOUT, send}
	cmd.Stderr = &Sender{util.STDERR, send}

	err := cmd.Start()
	if err != nil {
		log.Print("Process failed")
		send(util.Message{Code: util.END, Err: err})
		return
	}

	// Process stdin
	go func() {
		log.Printf("Processing stdin")
		defer func() {
			var err interface{}
			if err = recover(); err == nil {
				return
			}

			var closed bool
			if err.(error).Error() == "use of closed network connection" {
				closed = true
			}
			switch e := err.(type) {
			case *net.OpError:
				// Network connection may be closed?
				if e.Err.Error() == "use of closed network connection" {
					closed = true
				}
				log.Printf("Unknown network error? %#+v", err)
			default:
				log.Printf("Unknown error? %#+v", err)
			}
			if !closed {
				panic(err)
			}
			log.Println("Client closed connection")
		}()

		var m util.Message
		for {
			recv(&m)

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
				stdin.Close()

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
				log.Print("Process was signalled?!")
			}
			if !status.Exited() {
				log.Panicf("Process hasn't exited when it should have.. %#+v", err.ProcessState)
			}
			send(util.Message{Code: util.END, Reason: status.ExitStatus()})
			return
		default:
			panic(err)
		}
	}
	send(util.Message{Code: util.END})
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
	recv(&client_type)

	log.Printf("Got a new connection: %q", client_type)

	switch client_type {
	case "worker":
		HandleWorker(send, recv)
	case "job":
		HandleJob(send, recv)
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
