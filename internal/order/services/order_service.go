package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/order/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/order/repository"
)

type OrderService struct {
	repo          repository.OrderRepository
	kafkaProducer *kafka.Writer
	ordersTopic   string
	logger        *zap.SugaredLogger
}

func NewOrderService(
	repo repository.OrderRepository,
	kafkaProducer *kafka.Writer,
	ordersTopic string,
	logger *zap.SugaredLogger,
) *OrderService {
	return &OrderService{
		repo:          repo,
		kafkaProducer: kafkaProducer,
		ordersTopic:   ordersTopic,
		logger:        logger,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, req models.CreateOrderRequest) (*models.Order, error) {
	// Считаем общую сумму
	var totalAmount float64
	for _, item := range req.Items {
		totalAmount += item.Price * float64(item.Quantity)
	}

	order := &models.Order{
		ID:          uuid.New(),
		UserID:      req.UserID,
		Items:       req.Items,
		TotalAmount: totalAmount,
		Status:      models.StatusPending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.Create(ctx, order); err != nil {
		return nil, fmt.Errorf("create order: %w", err)
	}

	// Saga: отправляем событие OrderCreatedEvent в Kafka
	// чтобы Inventory Service мог зарезервировать товары
	event := models.OrderCreatedEvent{
		OrderID:     order.ID,
		UserID:      order.UserID,
		Items:       order.Items,
		TotalAmount: order.TotalAmount,
	}

	if err := s.publishEvent(ctx, "OrderCreated", event); err != nil {
		// Компенсация: если не удалось отправить событие, отменяем заказ
		s.logger.Errorf("Failed to publish OrderCreated event, compensating: %v", err)
		_ = s.repo.UpdateStatus(ctx, order.ID, models.StatusCancelled)
		return nil, fmt.Errorf("publish event: %w", err)
	}

	s.logger.Infof("Order created: %s", order.ID)
	return order, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id uuid.UUID) (*models.Order, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *OrderService) ListOrders(ctx context.Context) ([]models.Order, error) {
	return s.repo.List(ctx)
}

func (s *OrderService) CancelOrder(ctx context.Context, id uuid.UUID) error {
	order, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if order.Status == models.StatusPaid {
		return fmt.Errorf("cannot cancel paid order")
	}

	if err := s.repo.UpdateStatus(ctx, id, models.StatusCancelled); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	// Saga: отправляем событие для компенсации резерва
	event := models.OrderCancelledEvent{
		OrderID: id,
		Reason:  "Cancelled by user",
	}

	if err := s.publishEvent(ctx, "OrderCancelled", event); err != nil {
		s.logger.Errorf("Failed to publish OrderCancelled event: %v", err)
	}

	return nil
}

func (s *OrderService) HandleInventoryReserved(ctx context.Context, event models.InventoryReservedEvent) error {
	s.logger.Infof("🔄 Handling InventoryReserved for order %s", event.OrderID)

	if err := s.repo.UpdateStatus(ctx, event.OrderID, models.StatusReserved); err != nil {
		s.logger.Errorf("❌ Failed to update status to RESERVED: %v", err)
		return fmt.Errorf("update status: %w", err)
	}

	s.logger.Infof("Order status changed to RESERVED: %s", event.OrderID)

	paymentEvent := models.OrderPendingEvent{
		OrderID:     event.OrderID,
		UserID:      event.UserID,
		TotalAmount: event.TotalAmount,
	}

	if err := s.publishEvent(ctx, "OrderPending", paymentEvent); err != nil {
		s.logger.Errorf(" Failed to publish OrderPending: %v", err)
		// Не критично — статус уже обновлён
	}

	return nil
}

func (s *OrderService) HandleInventoryFailed(ctx context.Context, event models.InventoryFailedEvent) error {
	s.logger.Warnf("Handling InventoryFailed for order %s: %s", event.OrderID, event.Reason)

	// Отменяем заказ
	if err := s.repo.UpdateStatus(ctx, event.OrderID, models.StatusCancelled); err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	s.logger.Infof("Order cancelled due to inventory failure: %s", event.OrderID)
	return nil
}

func (s *OrderService) HandlePaymentProcessed(ctx context.Context, event models.PaymentProcessedEvent) error {
	s.logger.Infof("Handling PaymentProcessed for order %s", event.OrderID)
	return s.repo.UpdateStatus(ctx, event.OrderID, models.StatusPaid)
}

func (s *OrderService) publishEvent(ctx context.Context, eventType string, event interface{}) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Topic: s.ordersTopic,
		Key:   []byte(eventType),
		Value: eventBytes,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}

	if err := s.kafkaProducer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

	s.logger.Infof("Published event %s", eventType)
	return nil
}
