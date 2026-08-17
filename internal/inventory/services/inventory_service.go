package services

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/repository"
)

type InventoryService struct {
	repo           repository.InventoryRepository
	kafkaProducer  *kafka.Writer
	inventoryTopic string
	logger         *zap.SugaredLogger
}

func NewInventoryService(
	repo repository.InventoryRepository,
	kafkaProducer *kafka.Writer,
	inventoryTopic string,
	logger *zap.SugaredLogger,
) *InventoryService {
	return &InventoryService{repo: repo,
		kafkaProducer:  kafkaProducer,
		inventoryTopic: inventoryTopic,
		logger:         logger}
}

func (s *InventoryService) GetInventory(ctx context.Context, productID uuid.UUID) (*models.Inventory, error) {
	return s.repo.GetByProductID(ctx, productID)
}

func (s *InventoryService) ListInventory(ctx context.Context) ([]models.Inventory, error) {
	return s.repo.List(ctx)
}

func (s *InventoryService) CreateInventory(ctx context.Context, req models.CreateInventoryRequest) (*models.Inventory, error) {
	inventory := &models.Inventory{
		ProductID:        req.ProductID,
		Name:             req.Name,
		Quantity:         req.Quantity,
		Price:            req.Price,
		ReservedQuantity: 0,
		UpdatedAt:        time.Now(),
	}

	if err := s.repo.Create(ctx, inventory); err != nil {
		return nil, fmt.Errorf("create inventory: %w", err)
	}

	return inventory, nil
}

func (s *InventoryService) HandleOrderCreated(ctx context.Context, event models.OrderCreatedEvent) error {
	s.logger.Infof("Processing OrderCreated event: order_id=%s", event.OrderID)
	for _, item := range event.Items {
		inventory, err := s.repo.GetByProductID(ctx, item.ProductID)
		if err != nil {
			return s.handleFailure(ctx, event.OrderID, fmt.Sprintf("product %s not found", item.ProductID))
		}

		if inventory.AvailableQuantity() < item.Quantity {
			return s.handleFailure(ctx, event.OrderID, fmt.Sprintf("insufficient quantity for %s: available=%d, requested=%d",
				inventory.Name, inventory.AvailableQuantity(), item.Quantity))
		}
	}

	reservations := make(map[uuid.UUID]int)
	for _, item := range event.Items {
		reservations[item.ProductID] = item.Quantity
	}

	if err := s.repo.Reserve(ctx, reservations); err != nil {
		return s.handleFailure(ctx, event.OrderID, fmt.Sprintf("failed to reserve: %v", err))
	}

	succesEvent := models.InventoryReservedEvent{
		OrderID:     event.OrderID,
		UserID:      event.UserID,
		TotalAmount: event.TotalAmount,
	}

	if err := s.publishEvent(ctx, "InventoryReservedEvent", succesEvent); err != nil {
		s.logger.Errorf("Failed to publish InventoryReserved event: %v", err)
		_ = s.repo.Release(ctx, reservations)
		return err
	}

	s.logger.Infof("Successfully reserved inventory for order %s", event.OrderID)
	return nil
}

func (s *InventoryService) HandleOrderCancelled(ctx context.Context, event models.OrderCancelledEvent) error {
	s.logger.Infof("Processing OrderCancelled event: order_id=%s", event.OrderID)
	s.logger.Infof("Inventory released for order %s", event.OrderID)
	return nil
}

func (s *InventoryService) handleFailure(ctx context.Context, orderID uuid.UUID, reason string) error {
	s.logger.Warnf("Inventory reservation failed for order %s: %s", orderID, reason)
	failedEvent := models.InventoryFailedEvent{
		OrderID: orderID,
		Reason:  reason,
	}

	if err := s.publishEvent(ctx, "InventoryFailedEvent", failedEvent); err != nil {
		s.logger.Errorf("Failed to publish InventoryFailed event: %v", err)
	}

	return fmt.Errorf(reason)
}

func (s *InventoryService) publishEvent(ctx context.Context, eventType string, event interface{}) error {
	// Здесь будет реализация отправки в Kafka
	// Аналогично Order Service
	s.logger.Infof("Publishing event: %s", eventType)
	return nil
}
