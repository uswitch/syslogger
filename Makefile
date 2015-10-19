.phony: run
	
./bin/syslogger: $(wildcard src/syslogger/*.go)
	GOPATH=$(shell pwd) GO15VENDOREXPERIMENT=1 go install syslogger

run: ./bin/syslogger
	./bin/syslogger -readTimeout=5s