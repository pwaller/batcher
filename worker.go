package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
)

type Worker struct {
	Login
}

func (me *Worker) Connect(addr, via string) {
	var conn io.ReadWriteCloser
	conn, me.Login = Connect(addr, via, CLIENT_TYPE_WORKER)
	defer conn.Close()
	defer close(me.Send)

	log.Printf("Connected to server")
	defer log.Printf("Disconnected from server")

	if (<-me.Recv).Type != MESSAGE_TYPE_ACKNOWLEDGE {
		log.Panicf("First message from server was not an acknowledgement!")
	}

	for i := 0; i < *n_worker; i++ {
		me.Start()
	}

	signalled := make(chan os.Signal)
	signal.Notify(signalled, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGUSR1)

	for {
		select {
		case msg, ok := <-me.Recv:
			if !ok {
				// Connection closed
				return
			}

			switch msg.Type {
			case MESSAGE_TYPE_BROADCAST:
				// Broadcasts get run right now, with no queuing
				go func() {
					// log.Print("Processing broadcast")
					// defer log.Print(".. done")

					j := msg.Job
					err := me.RunJob(j)
					if err != nil {
						log.Printf("Broadcast job failed: %#+v", j)
					}
					close(j.Accepted)
					close(j.Done)
				}()
			default:
				log.Panicf("Unexpected message from server: %#+v", msg)
			}

		case s := <-signalled:
			log.Printf("Exiting due to %v", s)
			return
		}
	}
}

func (me *Worker) Start() {
	// Let the Broker know that there is a new place it can send jobs to
	worker := NewWorker{make(chan NewJob), make(chan NewJob)}

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
				err := me.RunJob(j)
				if err != nil {
					// TODO(pwaller): Pass error conditions on to client
					log.Printf("Starting job failed: %q", err)
				}
				// Notify all listeners on j.Done that j is finished
				close(j.Accepted)
				close(j.Done)
				log.Printf(" .. finished")

			case j, ok := <-worker.NewJobBroadcast:
				if !ok {
					log.Printf("Worker finished.")
					return
				}
				log.Printf("Running job: %v", j.Args)
				err := me.RunJob(j)
				if err != nil {
					// TODO(pwaller): Pass error conditions on to client
					log.Printf("Starting job failed: %q", err)
				}
				// Notify all listeners on j.Done that j is finished
				close(j.Accepted)
				close(j.Done)
				log.Printf(" .. finished")
			}
		}
	}()
}

func (w *Worker) RunJob(job NewJob) (err error) {

	defer func() {
		if err := recover(); err != nil {
			log.Printf("Error trying to run job: %v", err)
		}
	}()

	args := append([]string{fmt.Sprintf("--adjustment=%v", *nice)}, job.Args...)
	cmd := exec.Command("nice", args...)

	var stdin io.WriteCloser
	var stdout, stderr io.ReadCloser

	// TODO(pwaller): determine what type of pipe we're connected to, and
	// replicate that here (e.g, allocate a pty, direct both stdout/stderror there)
	if job.Stdin != nil {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return
		}
	}
	if job.Stdout != nil {
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return
		}
	}
	if job.Stderr != nil {
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return
		}
	}

	// Let the client know we've accepted the job and we're about to start
	// forwarding stdin/stdout
	job.Accepted <- true
	err = cmd.Start()
	if err != nil {
		return
	}

	// Start a goroutine to handle each pipe, where needed
	// Pipes are are closed by RecieveForWriter/SendForReader

	// TODO(pwaller): Figure out how to use or eliminate StdinDone
	var StdinDone sync.WaitGroup
	if stdin != nil {
		StdinDone.Add(1)
		go RecieveForWriter(&StdinDone, job.Stdin, stdin)
	}
	if stdout != nil {
		go SendForReader(job.Stdout, stdout)
	}
	if stderr != nil {
		go SendForReader(job.Stderr, stderr)
	}

	// Don't use cmd.Wait() because they close the pipes, which we don't want.
	state, err := cmd.Process.Wait()
	if err != nil {
		return
	}
	_ = state
	// TODO(pwaller): something with state..
	// For example, we should notify the client of the exit code, cpu usage.
	return
}
