# Syslogger

Syslogger is a tool to help forward Rsyslog messages to [Apache Kafka](https://kafka.apache.org).

[Apache Kafka](https://kafka.apache.org) is a "high-performance, distributed messaging system" that is well suited for the collation of both business and system event data. Please see Jay Kreps' wonderful ["The Log: What every software engineer should know about real-time data's unifying abstraction"](http://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) for more information.

Syslogger will help you forward syslog messages to Kafka. Messages are forwarded from [rsyslog](http://www.rsyslog.com/) over a TCP connection to syslogger. Rsyslog already has a bunch of stuff to make forwarding messages as reliable as possible, handling back-pressure and writing queued messages to disk etc. For more information please see ["Reliable Forwarding of syslog Messages with Rsyslog"](http://www.rsyslog.com/doc/rsyslog_reliable_forwarding.html).

## Design
Syslogger tries to be a good Rsyslog citizen by offloading as much responsibility for handling failure to Rsyslog. 

Reliability is achieved (as much as possible when using just TCP) by synchronously sending messages to Kafka: we put as much back-pressure onto Rsyslog as possible in the event of there being a problem or delay in forwarding messages to Kafka.

Syslogger starts a TCP listener, by default, on port 1514. It also attempts to connect to ZooKeeper to retrieve the connection details for the Kafka brokers. Metrics are collected using [go-metrics](https://github.com/rcrowley/go-metrics) and, because we use it a lot at uSwitch, forwarded to [Riemann](http://riemann.io).

## Building
Syslogger uses ZooKeeper so you'll need both the ZooKeeper library and headers available on your system.

    $ export GOPATH=$PWD
    $ go get
    $ go build

## Configuring Rsyslog
It's worth reading the Rsyslog documentation to make sure you configure Rsyslog according to your environment. If you just want to see stuff flowing on your development machine the following should suffice:

    $ActionQueueType LinkedList
    $ActionResumeRetryCount -1
    $ActionQueueFileName /tmp/syslog_queue
    $ActionQueueMaxFileSize 500M
    *.* @@localhost:1234
