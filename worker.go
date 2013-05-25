package main

import (
	"fmt"
	"log"
	"os/exec"
	"sync"
)

type Worker struct {
	Login
}

func (me *Worker) Connect(addr, via string) {

	me.Login = Connect(addr, via, CLIENT_TYPE_WORKER)

	log.Printf("Connected to server")
	defer log.Printf("Disconnected from server")

	if (<-me.Recv).Type != MESSAGE_TYPE_ACKNOWLEDGE {
		log.Panicf("First message from server was not an acknowledgement!")
	}

	for i := 0; i < *n_worker; i++ {
		me.Start()
	}

	for msg := range me.Recv {
		switch msg.Type {
		default:
			log.Panicf("Unexpected message type! %v", msg)
		}
	}

}

func (me *Worker) Start() {
	// Let the Broker know that there is a new place it can send jobs to
	worker := NewWorker{make(chan NewJob)}

	me.Send <- Message{
		Type:   MESSAGE_TYPE_NEW_WORKER,
		Worker: worker,
	}

	go func() {
		for {
			select {
			case j, ok := <-worker.NewJob:
				if !ok {
					log.Printf("Worker finished.")
					return
				}
				log.Printf("Running job: %v", j.Args)
				me.RunJob(j)
				// Notify all listeners on j.Done that j is finished
				close(j.Done)
				log.Printf(" .. finished")
			}
		}
	}()
}

func (w *Worker) RunJob(job NewJob) {

	defer func() {
		if err := recover(); err != nil {
			log.Printf("Error trying to run job: %v", err)
		}
	}()

	args := append([]string{fmt.Sprintf("--adjustment=%v", *nice)}, job.Args...)
	cmd := exec.Command("nice", args...)

	// TODO: determine what type of pipe we're connected to.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		panic(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	// Descriptors are closed by these goroutines

	job.Accepted <- true
	e := cmd.Start()
	if e != nil {
		panic(e)
	}

	var StdinDone sync.WaitGroup
	StdinDone.Add(1)
	go RecieveForWriter(&StdinDone, job.Stdin, stdin)
	go SendForReader(job.Stdout, stdout)
	go SendForReader(job.Stderr, stderr)

	state, err := cmd.Process.Wait()
	if err != nil {
		panic(err)
	}
	_ = state
	// TODO: something with state..
}
