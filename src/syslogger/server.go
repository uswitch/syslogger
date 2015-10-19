package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff"
	rfc3164 "github.com/jeromer/syslogparser/rfc3164"
	sh "github.com/pingles/go-metrics-stathat"
	"github.com/rcrowley/go-metrics"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	cfg         *config
	logger      = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
	port        = flag.Int("port", 1514, "port on which to listen")
	verbose     = flag.Bool("verbose", false, "print all messages to stdout")
	publish     = flag.Bool("publish", false, "publish messages to kafka")
	topic       = flag.String("topic", "syslog", "kafka topic to publish on")
	zkstring    = flag.String("zkstring", "localhost:2181", "ZooKeeper broker connection string")
	stathatEmail = flag.String("stathat", "", "StatHat.com email address")
	readTimeout  = flag.Duration("readTimeout", 1 * time.Minute, "timeout for reading messages from open syslog connections")

	connectionsCounter = metrics.NewCounter()
	sendMeter          = metrics.NewMeter()
	receivedMeter      = metrics.NewMeter()
	sendErrorsMeter    = metrics.NewMeter()
	sendDroppedMeter   = metrics.NewMeter()
	sendTimer          = metrics.NewTimer()
)

type config struct {
	port         int
	verbose      bool
	publish      bool
	topic        string
	zkstring     string
	stathatEmail string
	readTimeout  time.Duration
}

func (c *config) String() string {
	return fmt.Sprintf("port: %d, verbose: %t, publish: %t, topic: %s, zkstring: %s, stathat: %s, readTimeout: %s",
		c.port, c.verbose, c.publish, c.topic, c.zkstring, c.stathatEmail, c.readTimeout.String())
}

type openConnection struct {
	connection net.Conn
	done       chan bool
}

type server struct {
	listener    net.Listener
	connections []*openConnection
	client      sarama.Client
	producer    sarama.SyncProducer
	shutdown    chan bool
	readTimeout time.Duration
}

func (s *server) publishMessage(bytes []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: cfg.topic,
		Value: sarama.ByteEncoder(bytes),
	}

	start := time.Now()
	_, _, err := s.producer.SendMessage(msg)
	sendTimer.UpdateSince(start)

	if err != nil {
		logger.Println("error sending message, will retry.", err)
		sendErrorsMeter.Mark(1)
	} else {
		sendMeter.Mark(1)
	}

	return err
}

func (s *server) processLine(line []byte) {
	p := rfc3164.NewParser(line)
	if err := p.Parse(); err != nil {
		logger.Println("failed to parse:", err)
		return
	}

	receivedMeter.Mark(1)

	parts := p.Dump()

	jsonBytes, err := json.Marshal(parts)
	if err != nil {
		logger.Println("error marshaling message, skipping.", err)
		return
	}

	if cfg.verbose {
		logger.Println("enqueuing", string(jsonBytes))
	}

	if cfg.publish {
		put := func() error {
			return s.publishMessage(jsonBytes)
		}

		policy := backoff.NewExponentialBackOff()
		policy.MaxElapsedTime = time.Minute * 5
		err := backoff.Retry(put, policy)

		if err != nil {
			// retrying a bunch of times failed...
			logger.Println("failed sending message.", err)
			sendDroppedMeter.Mark(1)
		}
	}
}

func (s *server) handleConnection(conn *openConnection) {
	defer conn.connection.Close()

	connectionsCounter.Inc(1)
	defer connectionsCounter.Dec(1)
	
	logger.Println("got connection from:", conn.connection.RemoteAddr())
	logger.Println(connectionsCounter.Count(), "open connections")

	conn.connection.SetReadDeadline(time.Now().Add(s.readTimeout))

	scanner := bufio.NewScanner(conn.connection)
	for scanner.Scan() {
		b := []byte(scanner.Text())
		s.processLine(b)
	}

	if err := scanner.Err(); err != nil {
		logger.Println("error reading from connection:", err)
	}

	logger.Println("exiting connection handler")
	conn.done <- true
}

func (s *server) stop() {
	s.shutdown <- true
	s.listener.Close()

	for _, conn := range s.connections {
		logger.Println("closing connection to", conn.connection.RemoteAddr())
		conn.connection.Close()
	}

	logger.Println("waiting for", len(s.connections), "connections to close")
	for _, conn := range s.connections {
		<-conn.done
	}
	logger.Println("all connections closed")

	if cfg.publish {
		s.producer.Close()
		s.client.Close()
		logger.Println("finished stopping producer")
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
		client, producer, err := newProducerFromZookeeper()
		if err != nil {
			return err
		}
		s.client = client
		s.producer = producer
	}

	connections := make(chan *openConnection)
	errors := make(chan error)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				errors <- err
			} else {
				connection := &openConnection{
					connection: conn,
					done:       make(chan bool, 1),
				}
				connections <- connection
			}
		}
	}()

	logger.Println("waiting for syslog connection")
	for {
		select {
		case conn := <-connections:
			s.connections = append(s.connections, conn)
			go s.handleConnection(conn)
		case err := <-errors:
			logger.Println("failed to accept connection:", err)
		case <-s.shutdown:
			goto exit
		}
	}

exit:
	logger.Println("exiting listen loop")
	return nil
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
				logger.Println("refreshing kafka metadata")
				err := s.client.RefreshMetadata()
				if err != nil {
					logger.Println("error refreshing metadata.", err)
				}
				break
			}
		}
	}()
}

func registerMetrics() {
	metrics.Register("slogger.connections", connectionsCounter)
	metrics.Register("slogger.messages.sent", sendMeter)
	metrics.Register("slogger.messages.errors", sendErrorsMeter)
	metrics.Register("slogger.messages.dropped", sendDroppedMeter)
	metrics.Register("slogger.messages.time", sendTimer)
	metrics.Register("slogger.messages.received", receivedMeter)
}

func main() {
	flag.Parse()

	cfg = &config{
		port:         *port,
		verbose:      *verbose,
		publish:      *publish,
		topic:        *topic,
		zkstring:     *zkstring,
		stathatEmail: *stathatEmail,
		readTimeout:  *readTimeout,
	}

	logger.Println("starting with config:", cfg)

	registerMetrics()

	if cfg.stathatEmail != "" {
		logger.Println("sending metrics to stathat account", cfg.stathatEmail)
		go sh.StatHat(metrics.DefaultRegistry, time.Second * 10, cfg.stathatEmail)
	} else {
		logger.Println("metrics not being published")
		go metrics.Log(metrics.DefaultRegistry, time.Second * 10, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
	}

	server := &server{
		connections: []*openConnection{},
		shutdown:    make(chan bool),
		readTimeout: cfg.readTimeout,
	}

	handleInterrupt(server)

	if err := server.start(); err != nil {
		logger.Println("failed to listen:", err)
		os.Exit(1)
	}
}
