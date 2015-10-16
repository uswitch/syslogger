package main

import (
	"github.com/uswitch/kafkazk"
	"github.com/Shopify/sarama"
	"fmt"
)

func newProducerFromZookeeper() (sarama.Client, sarama.SyncProducer, error) {
	brokers, err := kafkazk.LookupBrokers(cfg.zkstring)
	if err != nil {
		return nil, nil, err
	}

	brokerStr := make([]string, len(brokers))
	for i, b := range brokers {
		brokerStr[i] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}

	logger.Println("connecting to Kafka, using brokers from ZooKeeper:", brokerStr)
	client, err := sarama.NewClient(brokerStr, sarama.NewConfig())
	if err != nil {
		return nil, nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, nil, err
	}

	return client, producer, nil
}

