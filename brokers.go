package main

import (
	"encoding/json"
	"fmt"
	"launchpad.net/gozk/zookeeper"
	"time"
)

type KafkaBroker struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func LookupBrokers(zkString string) ([]*KafkaBroker, error) {
	conn, channel, err := zookeeper.Dial(zkString, time.Second*15)

	brokers := []*KafkaBroker{}

	if err != nil {
		return nil, err
	}

	defer conn.Close()

	for event := range channel {
		if event.Type == zookeeper.EVENT_SESSION && event.State == zookeeper.STATE_CONNECTED {
			brokerIds, _, err := conn.Children("/brokers/ids")
			if err != nil {
				return nil, err
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
	return brokers, nil
}
