package main

import (
	"flag"
	"log"
	"net"
	"os"

	"github.com/kylelemons/fatchan"
)

// Flags
var (
	broker   = flag.Bool("broker", false, "Run as broker")
	worker   = flag.Bool("worker", false, "Run as worker")
	n_worker = flag.Int("N", 1, "Number of workers to start")
	nice     = flag.Int("nice", 15, "Niceness of workers")

	broadcast         = flag.Bool("broadcast", false, "Run on all workers")
	broadcast_machine = flag.Bool("broadcast-machine", false, "Run on all machines")
	addr              = flag.String("addr", ":1234", "Address on which to listen or connect")
	via               = flag.String("via", os.Getenv("BATCHER_VIA"), "Host to ssh via")
)

type ClientType int

const (
	CLIENT_TYPE_UNKNOWN ClientType = iota
	CLIENT_TYPE_WORKER
	CLIENT_TYPE_JOB
	CLIENT_TYPE_BROADCAST
)

type Login struct {
	Type ClientType
	Send chan Message `fatchan:"request"`
	Recv chan Message `fatchan:"reply"`
}

type MessageType int

const (
	MESSAGE_TYPE_UNKNOWN MessageType = iota
	MESSAGE_TYPE_ACKNOWLEDGE
	MESSAGE_TYPE_NEW_JOB
	MESSAGE_TYPE_NEW_WORKER
	MESSAGE_TYPE_BROADCAST
)

type Message struct {
	Type   MessageType
	Job    NewJob
	Worker NewWorker
}

type NewJob struct {
	Args []string

	Stdin  chan []byte `fatchan:"request"`
	Stdout chan []byte `fatchan:"reply"`
	Stderr chan []byte `fatchan:"reply"`

	Accepted chan bool `fatchan:"reply"`
	Done     chan bool `fatchan:"reply"`
}

type NewWorker struct {
	NewJob chan NewJob `fatchan:"reply"`
}

func main() {
	flag.Parse()

	switch {
	case *broker:
		NewServer().ListenAndServe(*addr)
	case *worker:
		(&Worker{}).Connect(*addr, *via)
	default:
		(&Client{}).Connect(*addr, *via, os.Args[1:])
	}
}

func Connect(addr, via string, client_type ClientType) Login {

	if via != "" {
		//log.Printf("Connecting to %v via %v", addr, via)
		panic("Unimplemented")
	} else {
		//log.Printf("Connecting to %v", addr)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial(%q): %s", addr, err)
	}

	xport := fatchan.New(conn, nil)
	login := make(chan Login)
	xport.FromChan(login)
	defer close(login)

	me := Login{
		Type: client_type,
		Send: make(chan Message),
		Recv: make(chan Message),
	}
	login <- me
	return me
}
