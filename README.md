    $ sudo apt-get install libzookeeper-mt-dev    


    $ export GOPATH=$PWD
    $ go get
    $ go build

inside /etc/rsyslog.d/60-forward.conf

    $ActionQueueType LinkedList
    $ActionResumeRetryCount -1
    $ActionQueueFileName /tmp/syslog_queue
    $ActionQueueMaxFileSize 500M
    *.* @@localhost:1234
