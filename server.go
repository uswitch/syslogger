package main

import (
	"bufio"
	"flag"
	"fmt"
	rfc3164 "github.com/jeromer/syslogparser/rfc3164"
	"net"
	"os"
	"os/signal"
)

type server struct {
	port        int
	connections []net.Conn
}

func process(line []byte) {
	p := rfc3164.NewParser(line)
	if err := p.Parse(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse:", err)
		return
	}

	for k, v := range p.Dump() {
		fmt.Println(k, ":", v)
	}
}

func handleConnection(conn net.Conn) {

	fmt.Fprintln(os.Stderr, "got connection from:", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		process([]byte(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading from connection:", err)
	}

}

func (s *server) handleInterrupt() {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)

	go func() {
		<-interrupts
		fmt.Fprintln(os.Stderr, "got interrupt signal, closing", len(s.connections), "connections")

		for _, conn := range s.connections {
			fmt.Fprintln(os.Stderr, "closing connection to", conn.RemoteAddr())
			conn.Close()
		}

		os.Exit(0)
	}()
}

var (
	port = flag.Int("port", 514, "port on which to listen")
)

func (s *server) start() error {
	s.handleInterrupt()

	fmt.Fprintf(os.Stderr, "starting to listen on %d\n", s.port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to accept connection:", err)
			continue
		}
		s.connections = append(s.connections, conn)
		go handleConnection(conn)
	}
}

func main() {
	flag.Parse()

	server := &server{
		port:        *port,
		connections: []net.Conn{},
	}

	if err := server.start(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to listen:", err)
		os.Exit(1)
	}
}
