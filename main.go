package main

// TODO(pwaller):
//
// * Pipes on terminals or disconnected pipes
// * Exit codes / signals
// * Transmit CWD
// * Transmit environment, perhaps?
// * Arguments separated by -- are separate jobs
// * Worker broadcast (as opposed to machine broadcast)
//
// Bugs:
//
//   b bash -c "cat ~/random_data | tee /dev/stderr | md5sum 2>&1" 3>&1 1>&2 2>&3 | md5sum
//   (md5 sums don't match, data is getting scrambled)
//
//
// Ideas:
//
//   Optionally prepend hostname to each line written
//
//   Named queues (a worker belongs to a particular name)
//
//   Could use a named worker queue as a way of negotiating access to a
//     resource, e.g, disk. For example, if you only want N workers to hit a
//     resource (disk?) simultaneously, each worker runs:
//       'batcher bash -c 'echo OK && kill -STOP "$$"'
//     continues when it sees "OK" and kills the batcher process when it's done.
//     .. OTOH, maybe NFS et al can already handle this?
//     .. batcher could also have a mode where it blocks until the job is
//	 	  accepted and then quits.

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"net/http"
	_ "net/http/pprof"
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

	pprof = flag.Bool("pprof", false, "Run profiler on port 6060")
)

type ClientType int

const (
	CLIENT_TYPE_UNKNOWN ClientType = iota
	CLIENT_TYPE_WORKER
	CLIENT_TYPE_JOB
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

	// Used to signal remote process, e.g, interrupt.
	// If closed, the running job (if any) is killed.
	Signal chan int `fatchan:"request"`

	Stdin  chan []byte `fatchan:"request"`
	Stdout chan []byte `fatchan:"reply"`
	Stderr chan []byte `fatchan:"reply"`

	Accepted chan bool `fatchan:"reply"`
	Done     chan bool `fatchan:"reply"`
	// Done is a channel which is closed when the job is finished through any
	// means.
}

type NewWorker struct {
	NewJob          chan NewJob `fatchan:"reply"`
	NewJobBroadcast chan NewJob `fatchan:"reply"`
}

func main() {
	flag.Parse()

	if *pprof {
		go func() {
			log.Println("Serving pprof on 6060..")
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

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
		c := Client{broadcast_machine: *broadcast_machine}
		c.Connect(*addr, *via, flag.Args())
	}
}
