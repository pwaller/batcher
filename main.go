package main

import (
	"flag"
	"log"

	"github.com/pwaller/batcher/broker"
	"github.com/pwaller/batcher/client"
	"github.com/pwaller/batcher/util"
	"github.com/pwaller/batcher/worker"
)

var start_broker = flag.Bool("broker", false, "Broker")
var start_worker = flag.Bool("worker", false, "Worker")

var nohupnice = flag.Bool("nohupnice", false, "nohup and nice")

var broker_addr = flag.String("baddr", "localhost", "Broker address")
var via = flag.String("via", "localhost", "Host to ssh via")

func main() {
	flag.Parse()

	if *start_broker {
		broker.Broker()
		return
	}

	// The difference between a secure connection and an unsecure one..
	//conn, err := net.Dial("tcp", *broker_addr+":1234")
	conn, err := util.SafeConnect(*via, *broker_addr, "1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() {
		log.Printf("Closing connection")
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
	if err != nil {
		panic(err)
	}
}
