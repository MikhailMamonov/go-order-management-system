package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewProducer(brokers []string) (*kafka.Writer, error) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		MaxAttempts:            3,
	}

	return writer, nil
}
