package main

import (
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/kylelemons/fatchan"
)

type Workers struct {
	m map[*chan Message]bool
	sync.Mutex
}

func NewWorkers() *Workers {
	return &Workers{m: map[*chan Message]bool{}}
}

func (ws *Workers) Add(c *chan Message) {
	ws.Lock()
	defer ws.Unlock()
	ws.m[c] = true
	log.Printf("New worker: %v total", len(ws.m))
}

func (ws *Workers) Remove(c *chan Message) {
	ws.Lock()
	defer ws.Unlock()
	delete(ws.m, c)
	log.Printf("Worker dropped: %v total", len(ws.m))
}

// Send Job in `m` to all workers simultaneously, waiting for them all to process
// otherwise timing out after 20 seconds
func (ws *Workers) StripeJobs(m Message) {
	stdout, stderr := m.Job.Stdout, m.Job.Stderr

	wg := sync.WaitGroup{}
	defer wg.Wait()

	for worker_recv := range ws.m {
		// Don't overwrite the outer scope
		// var m Message = m

		m.Job.Stdout = make(chan []byte)
		m.Job.Stderr = make(chan []byte)
		m.Job.Done = make(chan bool)
		m.Job.Accepted = make(chan bool)

		forward := func(out chan<- []byte, in <-chan []byte) {
			for data := range in {
				out <- data
			}
			wg.Done()
		}

		wg.Add(3) // 3 goroutines
		go forward(stdout, m.Job.Stdout)
		go forward(stderr, m.Job.Stderr)

		go func(m Message, Recv chan Message) {
			select {
			case Recv <- m:
				<-m.Job.Accepted
			case <-time.After(20 * time.Second):
				log.Printf("Worker was slow to respond")
			}
			<-m.Job.Done
			wg.Done()
		}(m, *worker_recv)
	}
}

type Server struct {
	// TODO(pwaller): Currently workers read from this (treating it as a queue)
	// However, instead we should have something central read from it and then
	// choose a worker based on its load.
	//
	// Current behaviour is a round-robin across connected workers in the order
	// in which they connect.
	RequestSlot chan chan NewJob
	Workers     Workers
}

func NewServer() *Server {
	return &Server{make(chan chan NewJob), Workers{m: map[*chan Message]bool{}}}
}

func (s *Server) ListenAndServe(addr string) {
	log.Printf("Serving..")
	defer log.Printf("Server ceasing..")

	listener, err := net.Listen("tcp4", addr)
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

			address := conn.RemoteAddr().(*net.TCPAddr)
			if !address.IP.IsLoopback() {
				log.Printf("Rejecting non-loopback connection!")
				_ = conn.Close()
				continue
			}

			if ok, err := CheckUser(address); !ok || err != nil {
				if err != nil {
					log.Printf("Error determining user from connection: %q", err)
				}
				log.Printf("Rejecting connection from different user! %v", address)
				_ = conn.Close()
				continue
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
	log.Printf("New client: %v", runtime.NumGoroutine())
	//defer log.Printf("Client %q disconnected", id)

	xport := fatchan.New(conn, nil)
	login := make(chan Login)
	_, err := xport.ToChan(login)
	if err != nil {
		return
	}

	client := <-login
	defer close(client.Recv)

	// See https://github.com/kylelemons/fatchan/issues/3
	// This is a workaround, the server sends something first.
	client.Recv <- Message{Type: MESSAGE_TYPE_ACKNOWLEDGE}

	switch client.Type {
	case CLIENT_TYPE_JOB:
		s.ServeJobRequest(client)

	case CLIENT_TYPE_WORKER:
		s.ServeWorker(client)

	default:
		log.Panicf("Bad client type! %v", client.Type)
	}
}

func (s *Server) ServeWorker(client Login) {
	s.Workers.Add(&client.Recv)
	defer s.Workers.Remove(&client.Recv)

	worker_closed := make(chan bool)
	defer close(worker_closed)

	for m := range client.Send {
		// Messages the worker is allowed to send to us
		// TODO(pwaller): Maybe extend this with worker heartbeat/load?
		switch m.Type {
		case MESSAGE_TYPE_NEW_WORKER:
			go s.FeedWorker(m.Worker, worker_closed)

		default:
			log.Panicf("Bad message type for worker! %v", m.Type)
		}
	}
}

// Obtain jobs from the job queue
func (s *Server) FeedWorker(worker NewWorker, worker_closed <-chan bool) {
	defer close(worker.NewJob)

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
			// Note: this is just waiting for a close, not a value.
			<-job.Done

		case <-worker_closed:
			return
		}
	}
}

func (s *Server) ServeJobRequest(client Login) {
	//log.Printf("Serving job request")
	//defer log.Printf("Client disconnected")

	for m := range client.Send {
		switch m.Type {
		case MESSAGE_TYPE_NEW_JOB:
			// A slot is a place where we can put a job.
			// Someone has to accept our slot before we can put a job onto it
			slot := make(chan NewJob)

			select {
			case s.RequestSlot <- slot:
				slot <- m.Job

			case msg, open := <-client.Send:
				if !open {
					// Client closed connection, cancelling request
					// Closes required to prevent goroutine leaks
					close(m.Job.Accepted)
					close(m.Job.Done)
					close(m.Job.Stdout)
					close(m.Job.Stderr)
					return
				}
				log.Panicf("Unexpected message type %v %v", msg.Type, msg)
				return
			}

		case MESSAGE_TYPE_BROADCAST:
			// Send a message to all workers
			log.Printf("Got broadcast")
			m.Job.Stdin = nil
			m.Job.Accepted <- true

			s.Workers.StripeJobs(m)

			close(m.Job.Stdout)
			close(m.Job.Stderr)
			close(m.Job.Accepted)
			close(m.Job.Done)
			log.Printf("Complete..")
			return

		default:
			log.Panicf("Bad message type for client! %v", m.Type)
		}
	}
}
