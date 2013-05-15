package main

import (
	"flag"
	"io"
	"log"
	"net"

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
		broker.Broker()
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
