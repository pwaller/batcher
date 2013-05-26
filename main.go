package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

// Flags
var (
	broker   = flag.Bool("broker", false, "Run as broker")
	worker   = flag.Bool("worker", false, "Run as worker")
	n_worker = flag.Int("N", 1, "Number of workers to start")
	nice     = flag.Int("nice", 15, "Niceness of workers")

	forward = flag.Bool("forward", false, "[internal]")

	daemon = flag.Bool("daemon", false, "Run as daemon")

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

	if *daemon {
		var args []string
		for _, a := range os.Args {
			if strings.HasPrefix(a, "-daemon") {
				continue
			}
			args = append(args, a)
		}
		cmd := exec.Command(args[0], args[1:]...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		// TODO(pwaller): Communicate to the child via a pipe and determine if
		// the connection was successful before daemonizing

		// cmd.ExtraFiles
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil

		log.Print("Starting as daemon")
		err := cmd.Start()
		if err != nil {
			log.Fatalf("Daemonize failed: %q", err)
		}
		return
	}

	switch {
	case *forward:
		Forward(flag.Args()[0])

	case *broker:
		NewServer().ListenAndServe(*addr)

	case *worker:
		(&Worker{}).Connect(*addr, *via)

	default:
		(&Client{}).Connect(*addr, *via, flag.Args())
	}
}
