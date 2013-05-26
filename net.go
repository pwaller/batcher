package main

import (
	"bytes"
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
	// "syscall"

	"github.com/kylelemons/fatchan"
)

// Return true if the other end of `addr` belongs to the same user as the server
// by inspecting the contents of /proc/net/tcp
func CheckUser(addr *net.TCPAddr) (ok bool, err error) {
	a := []byte(addr.IP)
	// reverse it
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
	ADDR := strings.ToUpper(hex.EncodeToString(a))
	the_address := ADDR + ":" + fmt.Sprintf("%04X", addr.Port)

	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		return
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		if len(fields) < 3 {
			continue
		}
		remote := fields[1]

		// This line represents this connection, check which user it belongs to
		if remote == the_address {
			user := fields[7]
			u, _ := strconv.Atoi(user)
			ok = u == os.Geteuid()
			return
		}
	}

	return
}

type SSHCloser struct {
	stdin, stdout io.Closer
}

func (c *SSHCloser) Close() (err error) {
	_ = c.stdin.Close()
	_ = c.stdout.Close()
	return
}

// Connect to `addr` by sshing through `via`. Assumes that batcher is in $PATH
// on the other end.
func SafeDial(via, addr string) (result io.ReadWriteCloser, err error) {
	// TODO(pwaller): Could implement this using go crypto
	// https://code.google.com/p/go/source/browse?repo=crypto#hg%2Fssh
	netcat := fmt.Sprintf("batcher -forward %v", addr)
	cmd := exec.Command("ssh", via, netcat)

	// Run in its own process group so that we don't get "Killed by signal" messages
	// (NOTE: This is commented out because if we die, the SSH process does not
	//        - a bad idea, lest we risk spamming everywhere with ssh processes)
	// cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

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
		return
	}
	var buf [5]byte
	n, err := stdout.Read(buf[:])
	if err != nil {
		return
	}

	if n != 5 || !bytes.Equal(buf[:], []byte("CONN\n")) {
		err = fmt.Errorf("Failed to connect via SSH: %q", string(buf[:]))
		return
	}

	go func() {
		// Note: can't use cmd.Wait here due to data races on the file
		//       descriptors being closed. Use cmd.Process.Wait() instead.
		// err := cmd.Wait()
		_, err := cmd.Process.Wait()
		if err != nil {
			// Crap, ssh bailed on us?
			log.Printf("ssh exited: %v", err)
		}
	}()
	return struct {
		io.Reader
		io.Writer
		io.Closer
	}{stdout, stdin, &SSHCloser{stdin, stdout}}, nil
}

// Primitive "nc" lookalike which emits "CONN" when there is a successful
// connection
func Forward(addr string) {
	// log.Printf("Connecting to %v", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("batcher -forward: %q", err)
		return
	}
	fmt.Println("CONN")

	stdin, stdout := make(chan bool), make(chan bool)
	cerr := make(chan error)

	go func() {
		_, err := io.Copy(conn, os.Stdin)
		if err != nil {
			cerr <- err
		}
		// log.Printf("Finished copy (stdin -> conn)")
		close(stdin)
	}()
	go func() {
		_, err := io.Copy(os.Stdout, conn)
		if err != nil {
			cerr <- err
		}
		// log.Printf("Finished copy (conn -> stdout)")
		close(stdout)
	}()

	// Wait for either side to be closed, then exit
	select {
	case <-stdin:
	case <-stdout:
	case err := <-cerr:
		log.Fatalf("Forward(%q) errored: %q", addr, err)
	}
}

// Connect to `addr` by sshing through `via` (if specified), telling the server
// that we are a `client_type` and passing an approprite Login struct
func Connect(addr, via string, client_type ClientType) (io.ReadWriteCloser, Login) {

	var conn io.ReadWriteCloser
	var err error

	if via != "" {
		conn, err = SafeDial(via, addr)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		log.Fatalf("Connect(%q, %q): %s", addr, via, err)
	}

	xport := fatchan.New(conn, nil)
	login := make(chan Login)
	_, err = xport.FromChan(login)
	if err != nil {
		log.Fatalf("Connect(%q, %q): %s", addr, via, err)
	}
	defer close(login)

	me := Login{
		Type: client_type,
		Send: make(chan Message),
		Recv: make(chan Message),
	}
	login <- me
	return conn, me
}
