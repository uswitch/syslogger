package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	rfc3164 "github.com/jeromer/syslogparser/rfc3164"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	cfg      *config
	logger   = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
	port     = flag.Int("port", 514, "port on which to listen")
	verbose  = flag.Bool("verbose", false, "print all messages to stdout")
	publish  = flag.Bool("publish", false, "publish messages to kafka")
	topic    = flag.String("topic", "syslog", "kafka topic to publish on")
	zkstring = flag.String("zkstring", "localhost:2181", "ZooKeeper broker connection string")
)

type config struct {
	port     int
	verbose  bool
	publish  bool
	topic    string
	zkstring string
}

func (c *config) String() string {
	return fmt.Sprintf("port: %d, verbose: %t, publish: %t, topic: %s, zkstring: %s",
		c.port, c.verbose, c.publish, c.topic, c.zkstring)
}

type server struct {
	listener    net.Listener
	connections []net.Conn
	client      *sarama.Client
	producer    *sarama.Producer
}

func (s *server) process(line []byte) {
	p := rfc3164.NewParser(line)
	if err := p.Parse(); err != nil {
		logger.Println("failed to parse:", err)
		return
	}

	parts := p.Dump()
	content := parts["content"].(string)

	if cfg.publish {
		if cfg.verbose {
			logger.Println("enqueuing", content)
		}

		err := s.producer.QueueMessage(cfg.topic,
			nil, sarama.StringEncoder(content))
		if err != nil {
			logger.Println("failed publishing", err)
		}
	}

	if cfg.verbose {
		logger.Println(content)
	}
}

func (s *server) handleConnection(conn net.Conn) {
	logger.Println("got connection from:", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		s.process([]byte(scanner.Text()))
	}

	if err := scanner.Err(); err != nil {
		logger.Println("error reading from connection:", err)
	}
}

func (s *server) stop() {
	s.listener.Close()

	for _, conn := range s.connections {
		logger.Println("closing connection to", conn.RemoteAddr())
		conn.Close()
	}

	if cfg.publish {
		s.client.Close()
		s.producer.Close()
	}
}

func (s *server) start() error {
	logger.Printf("starting to listen on %d\n", cfg.port)

	if cfg.verbose {
		logger.Println("verbose is enabled, all messages will be printed to stderr")
	}

	// listen for inbound syslog messages over tcp
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	if err != nil {
		return err
	}
	s.listener = listener

	if cfg.publish {
		// client for kafka
		brokers, err := LookupBrokers(cfg.zkstring)
		if err != nil {
			return err
		}

		brokerStr := make([]string, len(brokers))
		for i, b := range brokers {
			brokerStr[i] = fmt.Sprintf("%s:%d", b.Host, b.Port)
		}

		logger.Println("connecting to Kafka, using brokers from ZooKeeper:", brokerStr)
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
			logger.Println("failed to accept connection:", err)
			return err
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
				logger.Println("got kill signal, closing", len(s.connections), "connections")
				s.stop()
				os.Exit(0)
				break
			case syscall.SIGUSR1:
				// TODO: refresh/reload things?
				logger.Println("refreshing kafka endpoints?")
				break
			}
		}
	}()
}

func main() {
	flag.Parse()

	cfg = &config{
		port:     *port,
		verbose:  *verbose,
		publish:  *publish,
		topic:    *topic,
		zkstring: *zkstring,
	}

	logger.Println("starting with config:", cfg)

	server := &server{
		connections: []net.Conn{},
	}

	handleInterrupt(server)

	if err := server.start(); err != nil {
		logger.Println("failed to listen:", err)
		os.Exit(1)
	}
}
