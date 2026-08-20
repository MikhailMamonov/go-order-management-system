package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/payment/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/payment/services"
)

type OrderEventsConsumer struct {
	reader         *kafka.Reader
	paymentService *services.PaymentService
	logger         *zap.SugaredLogger
}

func NewOrderEventsConsumer(
	brokers []string,
	paymentService *services.PaymentService,
	logger *zap.SugaredLogger,
) *OrderEventsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "orders",
		GroupID:     "payment-service-group",
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})

	return &OrderEventsConsumer{reader: reader, paymentService: paymentService, logger: logger}
}

func (c *OrderEventsConsumer) Start(ctx context.Context) error {
	c.logger.Info("🚀 Starting OrderEventsConsumer for Payment Service")

	for {
		select {
		case <-ctx.Done():
			c.reader.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Errorf("❌ Error reading message: %v", err)
				time.Sleep(time.Second * 1)
				continue
			}

			c.logger.Infof("📨 Received message - Key: %s, Topic: %s, Offset: %d",
				string(msg.Key), msg.Topic, msg.Offset)

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Errorf("❌ Error processing message: %v", err)
			}
		}
	}
}

func (c *OrderEventsConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	eventType := string(msg.Key)

	switch eventType {
	case "OrderPending":
		var event models.OrderPendingEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal OrderPending: %w", err)
		}

		return c.paymentService.HandleOrderPending(ctx, event)
	default:
		c.logger.Debugf("Ignoring event type: %s", eventType)
		return nil
	}
}
