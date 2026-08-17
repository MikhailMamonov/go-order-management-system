package consumers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/services"
)

type OrderEventsConsumer struct {
	reader           *kafka.Reader
	inventoryService *services.InventoryService
	logger           *zap.SugaredLogger
}

func NewOrderEventsConsumer(
	brokers []string,
	inventoryService *services.InventoryService,
	logger *zap.SugaredLogger,
) *OrderEventsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "orders",
		GroupID:  "inventory-service",
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  100 * 1000,
	})

	return &OrderEventsConsumer{
		reader:           reader,
		inventoryService: inventoryService,
		logger:           logger,
	}
}

func (c *OrderEventsConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting OrderEventsConsumer")
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping consumer")
			return c.reader.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				c.logger.Errorf("Error reading message: %v", err)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Errorf("Error processing message: %v", err)
			}
		}
	}
}

func (c *OrderEventsConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	eventType := string(msg.Key)
	c.logger.Infof("Received event: %s", eventType)

	switch eventType {
	case "OrderCreated":
		var event models.OrderCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal OrderCreated: %w", err)
		}
		return c.inventoryService.HandleOrderCreated(ctx, event)
	case "OrderCanceled":
		var event models.OrderCancelledEvent
		if err := json.Unmarshal(msg.Value, event); err != nil {
			return fmt.Errorf("unmarshal OrderCanceled: %w", err)
		}
		return c.inventoryService.HandleOrderCancelled(ctx, event)
	default:
		c.logger.Debugf("Unknown event type: %s", eventType)
		return nil
	}
}
