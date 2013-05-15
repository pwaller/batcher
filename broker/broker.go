package broker

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/pwaller/batcher/util"
)

// (pwaller) design note:
// There are two connections which can go wrong. (client->broker broker->worker)
// This doesn't seem very robust to me.
// May need to introduce something which allows these to drop and possibly
// reconnect

type Broker struct {
	// TODO(pwaller) Maybe make this a priority queue so that jobs can have a
	// notion of priority?
	queue chan Job
}

func NewBroker() *Broker {
	return &Broker{make(chan Job)}
}

type Job struct {
	jobinfo    util.JobInfo
	send, recv func(interface{}) error
	done       chan bool
}

func (b *Broker) HandleWorker(send, recv func(interface{}) error) error {
	err := send("ok")
	if err != nil {
		return err
	}

	for {
		job := <-b.queue

		err = send(util.Message{Code: util.WORKER_NEWJOB, Job: job.jobinfo})
		if err != nil {
			panic(err)
		}

		done := make(chan bool)

		go func() {
			// Pump messages from the worker back to the client
			var m util.Message
			for {
				err := recv(&m)
				if err == io.EOF {
					log.Print("Worker closed the connection..")
					done <- true
					return
				}
				if err != nil {
					// TODO(pwaller): Handle this particular failure properly.
					// We should probably notify the client, for instance.
					log.Print("Worker dead?")
					done <- true
					panic(err)
				}

				err = job.send(m)
				if err != nil {
					// The client might not have heard us.
					// TODO Errors seen here: "broken pipe" (client)
					panic(err)
				}
				switch m.Code {
				case util.STDOUT, util.STDERR:
				case util.END:
					// The worker signalled to the client that was the end.
					// He's now ready for a new Job.
					done <- true
					return
				default:
					log.Panicf("Unhandled message code: %v", m.Code)
				}
			}
		}()

		go func() {
			// Pump messages from the client to the server
			var m util.Message
			for {
				err := job.recv(&m)
				if err == io.EOF {
					// Client disconnect
					log.Print("Client disconnect")
					return
				}

				if err != nil {
					return
					// TODO(pwaller): Handle this particular failure properly!
					// We should probably notify the worker, for instance.
					panic(err)
				}

				err = send(m)
				if err != nil {
					// The worker might not have heard us.
					panic(err)
				}

				switch m.Code {
				case util.STDIN, util.STDIN_CLOSED, util.SIGINT:
				default:
					log.Panicf("Unhandled message code: %v", m.Code)
				}

			}

		}()

		<-done
		job.done <- true

		// Let the worker's receive loop know this job is done
		err = send(util.Message{Code: util.END})
		if err != nil {
			panic(err)
		}

	}
	return nil
}

func (b *Broker) HandleJob(send, recv func(interface{}) error) error {
	err := send("ok")
	if err != nil {
		return err
	}

	var jobinfo util.JobInfo
	err = recv(&jobinfo)
	if err != nil {
		return err
	}

	log.Print("Broker recieved job: ", jobinfo)

	done := make(chan bool)
	b.queue <- Job{jobinfo, send, recv, done}
	<-done

	return nil
}

func (b *Broker) ServeOne(conn net.Conn) {
	defer conn.Close()

	defer func() {
		return
		if err := recover(); err != nil {
			// log.Printf("Panicked whilst server connection")
			// TODO(pwaller): something more graceful
			return
		}
	}()

	send, recv := util.Gobber(conn)

	var client_type string
	err := recv(&client_type)
	if err != nil {
		panic(err)
	}

	log.Printf("Got a new %v connection", client_type)

	switch client_type {
	case "worker":
		err := b.HandleWorker(send, recv)
		if err != nil {
			panic(err)
		}
	case "job":
		err := b.HandleJob(send, recv)
		log.Print("Job handled: ", err)
		if err != nil {
			panic(err)
		}
	default:
	}

}

func (b *Broker) Serve(l net.Listener) error {
	defer l.Close()
	var tempDelay time.Duration // how long to sleep on accept failure

	log.Print("Serving connections..")

	for {
		client, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Printf("Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0

		address := client.RemoteAddr().(*net.TCPAddr)
		if !address.IP.IsLoopback() {
			log.Printf("Rejecting non-loopback connection!")
			client.Close()
			continue
		}

		if !util.CheckUser(address) {
			log.Printf("Rejecting connection from different user!")
			client.Close()
			continue
		}

		go b.ServeOne(client)
	}
}

func BrokerServe() {
	l, e := net.Listen("tcp", "localhost:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	NewBroker().Serve(l)
}
