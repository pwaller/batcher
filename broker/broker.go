package broker

import (
	"log"
	"net"
	"time"

	"github.com/pwaller/batcher/util"
)

func HandleWorker(send, recv func(interface{}) error) error {
	send("ok")
	return nil
}

func HandleJob(send, recv func(interface{}) error) error {
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

	return nil
}

func ServeOne(conn net.Conn) {
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

	log.Printf("Got a new connection: %q", client_type)

	switch client_type {
	case "worker":
		err := HandleWorker(send, recv)
		if err != nil {
			panic(err)
		}
	case "job":
		err := HandleJob(send, recv)
		log.Print("Job handled: ", err)
		if err != nil {
			panic(err)
		}
	default:
	}

}

func Serve(l net.Listener) error {
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

		go ServeOne(client)
	}
}

func Broker() {
	l, e := net.Listen("tcp", "localhost:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	Serve(l)
}
