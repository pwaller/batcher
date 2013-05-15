package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/kr/pty"

	"github.com/pwaller/batcher/broker"
	"github.com/pwaller/batcher/client"
	"github.com/pwaller/batcher/util"
	"github.com/pwaller/batcher/worker"
)

var _ = net.FlagBroadcast

var start_broker = flag.Bool("broker", false, "Broker")
var start_worker = flag.Bool("worker", false, "Worker")

var nohupnice = flag.Bool("nohupnice", false, "nohup and nice")

var broker_addr = flag.String("baddr", "localhost", "Broker address")
var via = flag.String("via", "", "Host to ssh via")

func main() {
	flag.Parse()

	if *start_broker {
		broker.BrokerServe()
		return
	}

	if *start_worker && *nohupnice {
		var args []string
		for _, a := range os.Args {
			if strings.HasPrefix(a, "-nohupnice") {
				continue
			}
			args = append(args, a)
		}
		cmd := exec.Command("nice", args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		//cmd.Stdin = nil
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		//fd, _ := os.Open("/dev/null")
		//cmd.Stdout = fd
		//cmd.Stderr = fd
		//stdin, _ := cmd.StdinPipe()
		//_ = stdin
		//stdin.Close()

		log.Print("Hupping..")
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
		return
	}

	var conn io.ReadWriteCloser
	var err error

	if *via != "" {
		conn, err = util.SafeConnect(*via, *broker_addr, "1234")
	} else {
		conn, err = net.Dial("tcp", *broker_addr+":1234")
	}
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() {
		conn.Close()
	}()

	send, recv := util.Gobber(conn)

	if *start_worker {
		// Workaround: open a tty which can become the ctty if we don't have one.
		// Slightly annoying to waste one, but it's the simplest workaround
		pty.Open()

		err := worker.Worker(send, recv)
		if err != nil {
			panic(err)
		}
		return
	}

	err = client.NewJob(send, recv)
	if err == io.EOF {
		log.Print("Server disconnected")
	} else if err != nil {
		panic(err)
	}
}
