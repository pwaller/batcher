package main

import (
	"encoding/gob"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kless/terminal"
	"github.com/kr/pty"
)

func init() {
	gob.Register(exec.ExitError{})
	gob.Register(os.ProcessState{})
}

var broker = flag.Bool("broker", false, "Broker")
var worker = flag.Bool("worker", false, "Worker")

var nohupnice = flag.Bool("nohupnice", false, "nohup and nice")

var broker_addr = flag.String("baddr", "localhost", "Broker address")
var via = flag.String("via", "localhost", "Host to ssh via")

type Message struct {
	Code    Code
	Content []byte
	Err     error
	Reason  int
}

type Code int

const (
	STDIN Code = iota
	STDIN_CLOSED
	STDOUT
	STDERR

	SIGINT

	END
)

func gobber(channel io.ReadWriter) (func(interface{}), func(interface{})) {

	send := gob.NewEncoder(channel).Encode
	recv := gob.NewDecoder(channel).Decode

	// sends/recieves should behave as though they're atomic
	var smutex, rmutex sync.Mutex

	s := func(value interface{}) {
		smutex.Lock()
		defer smutex.Unlock()
		e := send(value)
		if e != nil {
			panic(e)
		}
	}
	r := func(value interface{}) {
		rmutex.Lock()
		defer rmutex.Unlock()
		e := recv(value)
		if e != nil {
			panic(e)
		}
	}

	return s, r
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

	send, recv := gobber(conn)

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

// Return true if the other end of `addr` belongs to the same user as the server
// Inspects the contents of /proc/net/tcp
func CheckUser(addr *net.TCPAddr) bool {
	a := []byte(addr.IP)
	// reverse it
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
	ADDR := strings.ToUpper(hex.EncodeToString(a))
	the_address := ADDR + ":" + fmt.Sprintf("%04X", addr.Port)
	//log.Print("User IP: ", the_address)

	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(data), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		if len(fields) < 3 {
			continue
		}
		remote := fields[1]

		if remote == the_address {
			user := fields[7]
			u, _ := strconv.Atoi(user)
			return u == os.Geteuid()
		}
	}

	return false
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

		if !CheckUser(address) {
			log.Printf("Rejecting connection from different user!")
			client.Close()
			continue
		}

		go ServeOne(client)
	}
}

func HandleWorker(send, recv func(interface{})) {
	send("ok")
}

func Worker(send, recv func(interface{})) error {
	send("worker")

	var response string
	recv(&response)
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}
	return nil
}

type Sender struct {
	code Code
	send func(interface{})
}

type JobInfo struct {
	HasTerm bool
}

func (s *Sender) Write(data []byte) (int, error) {
	log.Printf("Data: %q", string(data))
	s.send(Message{Code: s.code, Content: data})
	return len(data), nil
}

func HandleJob(send, recv func(interface{})) {
	send("ok")

	var args []string
	recv(&args)

	var jobinfo JobInfo
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

	cmd.Stdout = &Sender{STDOUT, send}
	cmd.Stderr = &Sender{STDERR, send}

	err := cmd.Start()
	if err != nil {
		log.Print("Process failed")
		send(Message{Code: END, Err: err})
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

		var m Message
		for {
			recv(&m)

			switch m.Code {
			case STDIN:
				// Here is something to feed to stdin..
				for len(m.Content) > 0 {
					n, err := stdin.Write(m.Content)
					m.Content = m.Content[n:]
					if err != nil {
						panic(err)
					}
				}

			case STDIN_CLOSED:
				stdin.Close()

			case SIGINT:
				cmd.Process.Signal(os.Interrupt)

			default:
				panic(fmt.Errorf("Unhandled code: %v", m.Code))
			}
		}
	}()

	err = cmd.Wait()
	log.Print("Process exited")

	if err != nil {
		switch err := err.(type) {
		case *exec.ExitError:
			// Unix only
			status := err.ProcessState.Sys().(syscall.WaitStatus)
			if !status.Exited() {
				log.Panicf("Process hasn't exited when it should have.. %#+v", err.ProcessState)
			}
			send(Message{Code: END, Reason: status.ExitStatus()})
			return
		default:
			panic(err)
		}
	}
	send(Message{Code: END})
}

// Client side of creating new job
func NewJob(send, recv func(interface{})) error {
	send("job")
	send(flag.Args())
	send(JobInfo{terminal.IsTerminal(int(os.Stdin.Fd()))})

	var response string
	recv(&response)
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}

	done := make(chan bool)

	// Handle stdin
	go func() {
		defer send(Message{Code: STDIN_CLOSED})

		var buf [10240]byte
		for {
			n, err := os.Stdin.Read(buf[:])
			//log.Printf("Read %v bytes from stdin", n)
			if err != nil {
				return
			}
			send(Message{Code: STDIN, Content: buf[:n]})
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		for {
			var v Message
			recv(&v)

			switch v.Code {
			case STDOUT:
				os.Stdout.Write(v.Content)
			case STDERR:
				os.Stderr.Write(v.Content)
			case END:
				done <- true
				return
			}
		}
	}()

	var last_interrupt time.Time

	for {
		select {
		case <-interrupt:
			send(Message{Code: SIGINT})
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

type MultiCloser []io.Closer

// Closes multiple closers, returns the error of the final one
// TODO(pwaller): Instead, return slice of errors, perhaps?
func (c *MultiCloser) Close() (err error) {
	for _, closer := range *c {
		e := closer.Close()
		if e != nil {
			err = e
		}
	}
	return
}

func SafeConnect(address, port string) (result io.ReadWriteCloser, err error) {
	netcat := fmt.Sprintf("nc %s %s 2> /dev/null", address, port)
	cmd := exec.Command("ssh", *via, netcat)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	go func() {
		err := cmd.Run()
		if err != nil {
			// Crap, ssh bailed on us.
			panic(err)
		}
	}()
	return struct {
		io.Reader
		io.Writer
		io.Closer
	}{stdout, stdin, &MultiCloser{stdin, stdout}}, nil
}

func Broker() {
	l, e := net.Listen("tcp", "localhost:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	Serve(l)
}

func main() {
	flag.Parse()

	if *broker {
		Broker()
		return
	}

	// The difference between a secure connection and an unsecure one..
	//conn, err := net.Dial("tcp", *broker_addr+":1234")
	conn, err := SafeConnect(*broker_addr, "1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() {
		log.Printf("Closing connection")
		conn.Close()
	}()

	send, recv := gobber(conn)

	if *worker {
		err := Worker(send, recv)
		if err != nil {
			panic(err)
		}
		return
	}

	err = NewJob(send, recv)
	if err != nil {
		panic(err)
	}
}
