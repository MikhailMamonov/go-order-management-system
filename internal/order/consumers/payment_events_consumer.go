package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/order/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/order/services"
)

type PaymentEventsConsumer struct {
	reader       *kafka.Reader
	orderService *services.OrderService
	logger       *zap.SugaredLogger
}

func NewPaymentEventsConsumer(brokers []string, orderService *services.OrderService, logger *zap.SugaredLogger) *PaymentEventsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "orders",
		GroupID:     "order-service-payment-group",
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})

	return &PaymentEventsConsumer{
		reader:       reader,
		orderService: orderService,
		logger:       logger,
	}
}

func (c *PaymentEventsConsumer) Start(ctx context.Context) error {
	c.logger.Info("🚀 Starting PaymentEventsConsumer")

	for {
		select {
		case <-ctx.Done():
			return c.reader.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Errorf("Error reading message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			c.logger.Infof("📨 Received payment message - Key: %s", string(msg.Key))

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Errorf("Error processing message: %v", err)
			}
		}
	}
}

func (c *PaymentEventsConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	eventType := string(msg.Key)

	switch eventType {
	case "PaymentProcessed":
		var event models.PaymentProcessedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal PaymentProcessed: %w", err)
		}

		return c.orderService.HandlePaymentProcessed(ctx, event)
	case "PaymentFailed":
		var event models.PaymentFailedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal PaymentFailed: %w", err)
		}

		return c.orderService.HandlePaymentFailed(ctx, event)
	default:
		c.logger.Debugf("Ignoring event type: %s", eventType)
		return nil
	}
}
