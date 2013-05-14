package worker

import (
	"fmt"
)

func Worker(send, recv func(interface{})) error {
	send("worker")

	var response string
	recv(&response)
	if response != "ok" {
		return fmt.Errorf("Got invalid response from server: %q", response)
	}
	return nil
}
