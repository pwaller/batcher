package util

import (
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
	Signal  os.Signal
}

type JobInfo struct {
	HasTerm bool
	Args    []string
}

type Code int

const (
	RESERVED Code = iota // to differentiate empty message

	STDIN
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

func Gobber(channel io.ReadWriter) (s, r func(interface{}) error) {

	send := gob.NewEncoder(channel).Encode
	recv := gob.NewDecoder(channel).Decode

	// sends/recieves should behave as though they're atomic
	var smutex, rmutex sync.Mutex

	s = func(value interface{}) (err error) {
		smutex.Lock()
		defer smutex.Unlock()
		err = send(value)
		return
	}
	r = func(value interface{}) (err error) {
		rmutex.Lock()
		defer rmutex.Unlock()
		err = recv(value)
		return
	}

	return
}

type SSHCloser struct {
	//proc *os.Process
	//cmd *exec.Cmd
	stdin, stdout io.Closer
}

func (c *SSHCloser) Close() (err error) {
	// Let SSH know we're done with him..
	//c.proc.Signal(syscall.SIGHUP)
	c.stdin.Close()
	c.stdout.Close()
	return
}

func SafeConnect(via, address, port string) (result io.ReadWriteCloser, err error) {
	netcat := fmt.Sprintf("nc %s %s 2> /dev/null", address, port)
	cmd := exec.Command("ssh", via, netcat)
	// Run in its own process group so that we don't get "Killed by signal" messages
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
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	go func() {
		//err := cmd.Wait()
		// Note: can't use cmd.Wait here due to data races on the file
		//       descriptors
		_, err := cmd.Process.Wait()
		if err != nil {
			// Crap, ssh bailed on us?
			//panic(err)
			log.Printf("ssh exited: %v", err)
		}
	}()
	return struct {
		io.Reader
		io.Writer
		io.Closer
	}{stdout, stdin, &SSHCloser{stdin, stdout}}, nil
}

func HandleNetClose() {
	var err interface{}
	if err = recover(); err == nil {
		return
	}

	var closed bool
	if err.(error).Error() == "use of closed network connection" {
		closed = true
	}
	switch e := err.(type) {
	case *net.OpError:
		// Network connection may be closed?
		if e.Err.Error() == "use of closed network connection" {
			closed = true
		} else {
			log.Printf("Unknown network error? %#+v", err)
		}
	default:
		log.Printf("Unknown error? %#+v", err)
	}
	if !closed {
		panic(err)
	}
	log.Println("Client closed connection")
}
