package util

import (
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type Message struct {
	Code    Code
	Content []byte
	Err     error
	Reason  int
}

type JobInfo struct {
	HasTerm bool
}

type Code int

const (
	STDIN Code = iota
	STDIN_CLOSED
	STDOUT
	STDERR

	SIGINT

	END
)

// Return true if the other end of `addr` belongs to the same user as the server
// Inspects the contents of /proc/net/tcp
func CheckUser(addr *net.TCPAddr) bool {
	a := []byte(addr.IP)
	// reverse it
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
	ADDR := strings.ToUpper(hex.EncodeToString(a))
	the_address := ADDR + ":" + fmt.Sprintf("%04X", addr.Port)
	//log.Print("User IP: ", the_address)

	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(data), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		if len(fields) < 3 {
			continue
		}
		remote := fields[1]

		if remote == the_address {
			user := fields[7]
			u, _ := strconv.Atoi(user)
			return u == os.Geteuid()
		}
	}

	return false
}

func Gobber(channel io.ReadWriter) (func(interface{}), func(interface{})) {

	send := gob.NewEncoder(channel).Encode
	recv := gob.NewDecoder(channel).Decode

	// sends/recieves should behave as though they're atomic
	var smutex, rmutex sync.Mutex

	s := func(value interface{}) {
		smutex.Lock()
		defer smutex.Unlock()
		e := send(value)
		if e != nil {
			panic(e)
		}
	}
	r := func(value interface{}) {
		rmutex.Lock()
		defer rmutex.Unlock()
		e := recv(value)
		if e != nil {
			panic(e)
		}
	}

	return s, r
}

type MultiCloser []io.Closer

// Closes multiple closers, returns the error of the final one
// TODO(pwaller): Instead, return slice of errors, perhaps?
func (c *MultiCloser) Close() (err error) {
	for _, closer := range *c {
		e := closer.Close()
		if e != nil {
			err = e
		}
	}
	return
}

func SafeConnect(via, address, port string) (result io.ReadWriteCloser, err error) {
	netcat := fmt.Sprintf("nc %s %s 2> /dev/null", address, port)
	cmd := exec.Command("ssh", via, netcat)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	go func() {
		err := cmd.Run()
		if err != nil {
			// Crap, ssh bailed on us.
			panic(err)
		}
	}()
	return struct {
		io.Reader
		io.Writer
		io.Closer
	}{stdout, stdin, &MultiCloser{stdin, stdout}}, nil
}
