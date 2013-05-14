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
	"strconv"
	"strings"
	"os/exec"
	"os"
	"time"
)

var broker = flag.Bool("broker", false, "Broker")
var worker = flag.Bool("worker", false, "Worker")

var nohupnice = flag.Bool("nohupnice", false, "nohup and nice")

var broker_addr = flag.String("baddr", "localhost", "Broker address")
var via = flag.String("via", "localhost", "Host to ssh via")

func gobber(channel io.ReadWriter) (func(interface{}), func(interface{})) {

	send := gob.NewEncoder(channel).Encode
	recv := gob.NewDecoder(channel).Decode

	s := func(value interface{}) {
		e := send(value)
		if e != nil {
			panic(e)
		}
	}
	r := func(value interface{}) {
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

	log.Print("Serving connections..")

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

		//log.Printf("Accepted connection from %#+v to %#+v", client.RemoteAddr(), client.LocalAddr())
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

func (s Sender) Write(data []byte) (int, error) {
	println(string(data))
	s.send(s.code)
	s.send(data)
	return len(data), nil
}

func HandleJob(send, recv func(interface{})) {
	send("ok")
	var args []string
	recv(&args)

	log.Printf("Got arguments to run: %v", args)

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = Sender{STDOUT, send}
	cmd.Stderr = Sender{STDERR, send}

	err := cmd.Start()
	if err != nil {
		send(END)
		send(err)
	}

	result := cmd.Wait()
	send(END)
	send(result)
}

type Code int

const (
	STDOUT Code = iota
	STDERR
	END
)

func NewJob(send, recv func(interface{})) error {
	send("job")

	var response string
	recv(&response)
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}

	send(flag.Args())

	return nil
}

func SafeConnect(address, port string) (result io.ReadWriter, err error) {
	cmd := exec.Command("ssh", *via, "nc", address, port)
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
	}{stdout, stdin}, nil
}

func main() {
	flag.Parse()

	if *broker {
		l, e := net.Listen("tcp", "localhost:1234")
		if e != nil {
			log.Fatal("listen error:", e)
		}
		Serve(l)
		return
	}

	// The difference between a secure connection and an unsecure one..
	//conn, err := net.Dial("tcp", *broker_addr + ":1234")
	conn, err := SafeConnect(*broker_addr, "1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

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
