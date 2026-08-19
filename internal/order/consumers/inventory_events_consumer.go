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

type InventoryEventsConsumer struct {
	reader       *kafka.Reader
	orderService *services.OrderService
	logger       *zap.SugaredLogger
}

func NewInventoryConsumer(brokers []string, orderService *services.OrderService, logger *zap.SugaredLogger) *InventoryEventsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "inventory",
		GroupID:     "order-service-group-v3",
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	return &InventoryEventsConsumer{
		reader:       reader,
		orderService: orderService,
		logger:       logger,
	}
}

func (c *InventoryEventsConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting InventoryEventsConsumer")
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

			c.logger.Infof("📨 Received message - Key: %s, Value: %s", string(msg.Key), string(msg.Value))

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Errorf("❌ Error processing message: %v", err)
			}
		}
	}
}

func (c *InventoryEventsConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	eventType := string(msg.Key)

	switch eventType {
	case "InventoryReserved":
		var event models.InventoryReservedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal InventoryReserved: %w", err)
		}

		return c.orderService.HandleInventoryReserved(ctx, event)
	case "InventoryFailed":
		var event models.InventoryFailedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal InventoryFailed: %w", err)
		}

		return c.orderService.HandleInventoryFailed(ctx, event)
	case "PaymentProcessed":
		var event models.PaymentProcessedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal PaymentProcessed: %w", err)
		}

		return c.orderService.HandlePaymentProcessed(ctx, event)
	default:
		c.logger.Debugf("Unknown event type: %s", eventType)
		return nil
	}
}
