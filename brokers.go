package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"launchpad.net/gozk/zookeeper"
	"time"
)

type KafkaBroker struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

var noBrokersError = errors.New("brokers: no brokers found in zookeeper")

func LookupBrokers(zkString string) ([]*KafkaBroker, error) {
	logger.Println("connecting to zookeeper at", zkString)
	conn, channel, err := zookeeper.Dial(zkString, time.Second*15)

	brokers := []*KafkaBroker{}

	if err != nil {
		return nil, err
	}

	defer conn.Close()

	// TODO: should we use timeout channel too?
	for event := range channel {
		if event.Type == zookeeper.EVENT_SESSION && event.State == zookeeper.STATE_CONNECTED {
			brokerIds, _, err := conn.Children("/brokers/ids")
			if err != nil {
				return nil, err
			}

			if len(brokerIds) == 0 {
				return nil, noBrokersError
			}

			for _, bid := range brokerIds {
				path := fmt.Sprintf("/brokers/ids/%s", bid)
				data, _, err := conn.Get(path)
				if err != nil {
					return nil, err
				}

				var b KafkaBroker
				err = json.Unmarshal([]byte(data), &b)
				if err != nil {
					return nil, err
				}
				brokers = append(brokers, &b)
			}

			return brokers, nil
		}
	}

	return nil, errors.New("brokers: could not connect to zookeeper")
}
