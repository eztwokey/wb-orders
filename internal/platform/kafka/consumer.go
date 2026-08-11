package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewConsumer(brokers []string, clientID, group string, topics []string, rebalanceTimeout time.Duration) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(rebalanceTimeout),
		kgo.SessionTimeout(30*time.Second),
		kgo.FetchMaxWait(time.Second),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return client, nil
}
