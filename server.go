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
	"log"
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
	logger      *log.Logger
}

func (s *server) process(line []byte) {
	p := rfc3164.NewParser(line)
	if err := p.Parse(); err != nil {
		s.logger.Println("failed to parse:", err)
		return
	}

	parts := p.Dump()

	if s.config.publish {
		err := s.producer.SendMessage(s.config.topic,
			nil, sarama.StringEncoder(parts["content"].(string)))
		if err != nil {
			s.logger.Println("failed publishing", err)
		}
	}

	if s.config.debug {
		for k, v := range parts {
			s.logger.Println(k, ":", v)
		}
	}
}

func (s *server) handleConnection(conn net.Conn) {
	s.logger.Println("got connection from:", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		s.process([]byte(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		s.logger.Println("error reading from connection:", err)
	}
}

func (s *server) stop() {
	s.listener.Close()

	for _, conn := range s.connections {
		s.logger.Println("closing connection to", conn.RemoteAddr())
		conn.Close()
	}

	if s.config.publish {
		s.client.Close()
		s.producer.Close()
	}
}

func (s *server) start() error {
	s.logger.Printf("starting to listen on %d\n", s.config.port)
	
	if s.config.debug {
		s.logger.Println("debug is enabled, all messages will be printed to stderr")
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
		s.logger.Println("connecting to Kafka, using brokers from ZooKeeper:", brokerStr)
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
		// TODO
		// We get errors here when killing the app with Ctrl-C:
		// server.go:134: failed to accept connection: use of closed network connection
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Println("failed to accept connection:", err)
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
				s.logger.Println("got kill signal, closing", len(s.connections), "connections")
				s.stop()
				os.Exit(0)
				break
			case syscall.SIGUSR1:
				// TODO: refresh/reload things?
				s.logger.Println("refreshing kafka endpoints?")
				break
			}
		}
	}()
}

func main() {
	flag.Parse()
	
	logger := log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
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
		logger:      logger,
	}

	handleInterrupt(server)

	if err := server.start(); err != nil {
		server.logger.Println("failed to listen:", err)
		os.Exit(1)
	}
}
