package main

import (
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/kylelemons/fatchan"
)

type Server struct {
	RequestSlot chan chan NewJob
}

func NewServer() *Server {
	return &Server{make(chan chan NewJob)}
}

func (s *Server) ListenAndServe(addr string) {
	log.Printf("Serving..")
	defer log.Printf("Server ceasing..")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen(%q): %s", addr, err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf("accept(): %s", err)
			}

			go s.ServeOne(conn.RemoteAddr().String(), conn)
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGUSR1)
	switch <-c {
	case syscall.SIGUSR1:
		log.Printf("Unimplemented: safe restart")
	default:
	}
}

func (s *Server) ServeOne(id string, conn io.ReadWriteCloser) {
	log.Printf("Client %q connected", id)
	defer log.Printf("Client %q disconnected", id)

	xport := fatchan.New(conn, nil)
	login := make(chan Login)
	xport.ToChan(login)

	client := <-login
	defer close(client.Recv)

	switch client.Type {
	case CLIENT_TYPE_BROADCAST:
		panic("Unimplemented")

	case CLIENT_TYPE_JOB:
		s.ServeJobRequest(client)

	case CLIENT_TYPE_WORKER:
		s.ServeWorker(client)

	default:
		log.Panicf("Bad client type! %v", client.Type)
	}
}

func (s *Server) ServeWorker(client Login) {
	log.Printf("Serving worker")
	defer log.Printf("Client disconnected")

	// See https://github.com/kylelemons/fatchan/issues/3
	// This is a workaround, the server sends something first.
	client.Recv <- Message{Type: MESSAGE_TYPE_ACKNOWLEDGE}

	worker_closed := make(chan bool)

	for m := range client.Send {
		switch m.Type {
		case MESSAGE_TYPE_NEW_WORKER:
			go s.FeedWorker(m.Worker, worker_closed)

		default:
			log.Panicf("Bad message type for worker! %v", m.Type)
		}
	}

	close(worker_closed)
}

// Obtain jobs from the job queue
func (s *Server) FeedWorker(worker NewWorker, worker_closed <-chan bool) {
	for {
		// Read from the broker request queue, fetch the job and give it to the
		// worker.
		select {
		case slot := <-s.RequestSlot:
			job, ok := <-slot
			if !ok {
				// Job might have been cancelled.
				// I didn't choose to use this though, so it shouldn't happen.
				panic("Problem. See source.")
			}
			worker.NewJob <- job

			// Wait until the job is done before proceeding to the next one
			<-job.Done
		case <-worker_closed:
			return
		}
	}
}

func (s *Server) ServeJobRequest(client Login) {
	log.Printf("Serving job request")
	defer log.Printf("Client disconnected")

	client.Recv <- Message{Type: MESSAGE_TYPE_ACKNOWLEDGE}

	for m := range client.Send {
		switch m.Type {
		case MESSAGE_TYPE_NEW_JOB:
			log.Printf("Client requested job be done..")

			// A slot is a place where we can put a job.
			// Someone has to accept our slot before we can put a job onto it
			slot := make(chan NewJob)

			select {
			case s.RequestSlot <- slot:
				slot <- m.Job

			case msg, open := <-client.Send:
				if !open {
					// Client closed connection, cancelling request
					return
				}
				log.Panicf("Unexpected message type %v %v", msg.Type, msg)
				return
			}

		default:
			log.Panicf("Bad message type for client! %v", m.Type)
		}
	}
}
