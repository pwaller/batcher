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
	broadcast_machine, async bool
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

	// nil chans represent /dev/null
	var stdin, stdout, stderr chan []byte

	// TODO(pwaller): Figure out where os.Std* are pointing, and if they're all
	//                pointing at the same terminal.
	if !me.async {
		stdin = make(chan []byte)
		stdout = make(chan []byte)
		stderr = make(chan []byte)
	}

	job := NewJob{
		args,
		me.async,
		make(chan int),
		stdin, stdout, stderr,
		make(chan bool),
		make(chan bool),
	}

	// TODO(pwaller): For non-broadcast jobs, split Args on "--" and submit
	//                one job per set of arguments.
	mtype := MESSAGE_TYPE_NEW_JOB
	if me.broadcast_machine {
		mtype = MESSAGE_TYPE_BROADCAST
	}

	// log.Printf("Requesting new job: %v", args)
	me.Send <- Message{Type: mtype, Job: job}

	// Wait for the job to be accepted before we start reading in stdin
	<-job.Accepted

	if stdin != nil {
		go SendForReader(stdin, os.Stdin)
	}

	// Receivers need to terminate before main(), otherwise we might close
	// before we've received everything from the other side.
	var Receivers sync.WaitGroup
	defer Receivers.Wait()

	if stdout != nil {
		Receivers.Add(1)
		go RecieveForWriter(&Receivers, stdout, os.Stdout)
	}

	if stderr != nil {
		Receivers.Add(1)
		// Don't allow remote to close stderr since that's where we're reporting
		// information to with log.*.
		go RecieveForWriter(&Receivers, stderr, &WriterNopCloser{os.Stderr})
	}

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
			// TODO(pwaller): This shouldn't be seen during normal operation
			log.Println("Server dropped..")
			return
		}
	}
}
