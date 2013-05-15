package worker

import (
	"fmt"
	"io"
	"log"

	"github.com/pwaller/batcher/util"
)

func Worker(send, recv func(interface{}) error) error {
	err := send("worker")
	if err != nil {
		return err
	}

	var response string
	err = recv(&response)
	if err != nil {
		return err
	}
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}

	var m util.Message
	for {
		err := recv(&m)
		if err == io.EOF {
			log.Print("Broker closed connection")
			return nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}
