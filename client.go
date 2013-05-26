package main

import (
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Client struct {
	broadcast_machine bool
	Login
}

// Drop closes
type WriterNopCloser struct {
	io.WriteCloser
}

func (w *WriterNopCloser) Close() error { return nil }

func (me *Client) Connect(addr, via string, args []string) {

	var conn io.ReadWriteCloser
	conn, me.Login = Connect(addr, via, CLIENT_TYPE_JOB)
	defer conn.Close()
	defer close(me.Send)

	// log.Printf("Connected to server")
	// defer log.Printf("Disconnected from server")

	if (<-me.Recv).Type != MESSAGE_TYPE_ACKNOWLEDGE {
		log.Panicf("First message from server was not an acknowledgement!")
	}

	job := NewJob{
		args,
		// nil, TODO(pwaller): I have tested that nil chans are represented as
		// nil chans on the other end of the connection, so we can use this to
		// represent reading/writing from/to /dev/null
		make(chan int),
		make(chan []byte),
		make(chan []byte),
		make(chan []byte),
		make(chan bool),
		make(chan bool),
	}

	mtype := MESSAGE_TYPE_NEW_JOB
	if me.broadcast_machine {
		mtype = MESSAGE_TYPE_BROADCAST
	}

	// log.Printf("Requesting new job: %v", args)
	me.Send <- Message{Type: mtype, Job: job}

	// Wait for the job to be accepted before we start reading in stdin
	<-job.Accepted

	// TODO(pwaller): Figure out where os.Std* are pointing, and if they're all
	//                pointing at the same terminal.

	go SendForReader(job.Stdin, os.Stdin)

	var Receivers sync.WaitGroup
	Receivers.Add(2)
	defer Receivers.Wait()

	go RecieveForWriter(&Receivers, job.Stdout, os.Stdout)
	// Don't allow remote to close stderr since that's where we're reporting
	// information to.
	go RecieveForWriter(&Receivers, job.Stderr, &WriterNopCloser{os.Stderr})

	connection_finished := make(chan bool)
	go func() {
		for msg := range me.Recv {
			log.Printf("Server sent me a message! %v", msg)
			panic("This shouldn't happen.")
		}
		connection_finished <- true
	}()

	signalled := make(chan os.Signal)
	signal.Notify(signalled, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGUSR1)

	for {
		// log.Println("Awaiting signal")
		select {
		case <-job.Done:
			return

		case <-signalled:
			log.Print("Signalled?")
			return

		case <-connection_finished:
			log.Println("Server dropped..")
			return
		}
	}
}
