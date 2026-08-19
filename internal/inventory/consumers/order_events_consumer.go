package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
		Brokers:     brokers,
		Topic:       "orders",
		GroupID:     "inventory-service-v2",
		MinBytes:    10e3,
		MaxBytes:    10e6,
		MaxWait:     100 * 1000,
		StartOffset: kafka.FirstOffset,
	})

	return &OrderEventsConsumer{
		reader:           reader,
		inventoryService: inventoryService,
		logger:           logger,
	}
}

func (c *OrderEventsConsumer) Start(ctx context.Context) error {
	c.logger.Infof("🚀 Starting OrderEventsConsumer")
	c.logger.Infof("   Topic: orders")
	c.logger.Infof("   Brokers: %v", c.reader.Config().Brokers)
	c.logger.Infof("   GroupID: %s", c.reader.Config().GroupID)

	// Проверяем подключение к Kafka
	for _, broker := range c.reader.Config().Brokers {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			c.logger.Errorf("❌ Cannot connect to Kafka broker %s: %v", broker, err)
			return err
		}
		c.logger.Infof("✅ Connected to Kafka broker: %s", broker)

		// Проверяем что topic существует
		partitions, err := conn.ReadPartitions("orders")
		if err != nil {
			c.logger.Errorf("❌ Cannot read partitions for topic 'orders': %v", err)
			conn.Close()
			return err
		}
		c.logger.Infof("✅ Topic 'orders' has %d partitions", len(partitions))
		conn.Close()
		break
	}

	c.logger.Info("👂 Waiting for messages from Kafka...")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping consumer")
			return c.reader.Close()
		default:
			// Читаем сообщение
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Errorf("❌ Error reading message from Kafka: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			c.logger.Infof("📨 RECEIVED MESSAGE!")
			c.logger.Infof("   Topic: %s", msg.Topic)
			c.logger.Infof("   Partition: %d", msg.Partition)
			c.logger.Infof("   Offset: %d", msg.Offset)
			c.logger.Infof("   Key: %s", string(msg.Key))
			c.logger.Infof("   Value: %s", string(msg.Value))

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Errorf("❌ Error processing message: %v", err)
			} else {
				c.logger.Infof("✅ Message processed successfully")
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
