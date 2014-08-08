package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	rfc3164 "github.com/jeromer/syslogparser/rfc3164"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	port     = flag.Int("port", 514, "port on which to listen")
	debug    = flag.Bool("debug", false, "print all messages to stdout")
	publish  = flag.Bool("publish", false, "publish messages to kafka")
	topic    = flag.String("topic", "syslog", "kafka topic to publish on")
	zkstring = flag.String("zkstring", "localhost:2181", "ZooKeeper broker connection string")
)

type config struct {
	port     int
	debug    bool
	publish  bool
	topic    string
	zkstring string
}

type server struct {
	config      *config
	listener    net.Listener
	connections []net.Conn
	client      *sarama.Client
	producer    *sarama.Producer
}

func (s *server) process(line []byte) {
	p := rfc3164.NewParser(line)
	if err := p.Parse(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to parse:", err)
		return
	}

	parts := p.Dump()

	if s.config.publish {
		err := s.producer.SendMessage(s.config.topic,
			nil, sarama.StringEncoder(parts["content"].(string)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed publishing to")
		}
	}

	if s.config.debug {
		for k, v := range parts {
			fmt.Println(k, ":", v)
		}
	}
}

func (s *server) handleConnection(conn net.Conn) {
	fmt.Fprintln(os.Stderr, "got connection from:", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		s.process([]byte(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading from connection:", err)
	}
}

func (s *server) stop() {
	s.listener.Close()

	for _, conn := range s.connections {
		fmt.Fprintln(os.Stderr, "closing connection to", conn.RemoteAddr())
		conn.Close()
	}

	if s.config.publish {
		s.client.Close()
		s.producer.Close()
	}
}

func (s *server) start() error {
	fmt.Fprintf(os.Stderr, "starting to listen on %d\n", s.config.port)
	if s.config.debug {
		fmt.Fprintln(os.Stderr, "debug is enabled, all messages will be printed to stderr")
	}

	// listen for inbound syslog messages over tcp
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.port))
	if err != nil {
		return err
	}
	s.listener = listener

	if s.config.publish {
		// client for kafka
		// TODO: lookup broker adresses from zookeeper
		brokers, err := LookupBrokers(s.config.zkstring)
		if err != nil {
			return err
		}
		brokerStr := make([]string, len(brokers))
		for i, b := range brokers {
			brokerStr[i] = fmt.Sprintf("%s:%d", b.Host, b.Port)
		}
		fmt.Println("Looking up Kafka brokers from ZooKeeper:", brokerStr)
		client, err := sarama.NewClient("syslog", brokerStr, sarama.NewClientConfig())
		if err != nil {
			return err
		}
		s.client = client

		// producer for kafka
		producer, err := sarama.NewProducer(client, nil)
		if err != nil {
			return err
		}
		s.producer = producer
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to accept connection:", err)
			continue
		}
		s.connections = append(s.connections, conn)
		go s.handleConnection(conn)
	}
}

func handleInterrupt(s *server) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGUSR1)

	go func() {
		for sig := range signals {
			switch sig {
			case syscall.SIGINT:
				fmt.Fprintln(os.Stderr, "got kill signal, closing", len(s.connections), "connections")
				s.stop()
				os.Exit(0)
				break
			case syscall.SIGUSR1:
				// TODO: refresh/reload things?
				fmt.Println("refreshing kafka endpoints?")
				break
			}
		}
	}()
}

func main() {
	flag.Parse()

	config := &config{
		port:     *port,
		debug:    *debug,
		publish:  *publish,
		topic:    *topic,
		zkstring: *zkstring,
	}

	server := &server{
		config:      config,
		connections: []net.Conn{},
	}

	handleInterrupt(server)

	if err := server.start(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to listen:", err)
		os.Exit(1)
	}
}
