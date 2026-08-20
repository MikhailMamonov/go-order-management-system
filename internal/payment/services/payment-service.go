package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/payment/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/payment/repository"
)

type PaymentService struct {
	repo            repository.PaymentRepository
	topic           string
	paymentProducer *kafka.Writer
	logger          *zap.SugaredLogger
}

func NewPaymentService(
	repo repository.PaymentRepository,
	kafkaProducer *kafka.Writer,
	paymentTopic string,
	logger *zap.SugaredLogger,
) *PaymentService {
	return &PaymentService{
		repo:            repo,
		topic:           paymentTopic,
		paymentProducer: kafkaProducer,
		logger:          logger,
	}
}

func (s *PaymentService) HandleOrderPending(ctx context.Context, event models.OrderPendingEvent) error {
	s.logger.Infof("Processing OrderPending event: order_id=%s, amount=%.2f", event.OrderID, event.TotalAmount)

	payment := models.Payment{
		ID:          uuid.New(),
		OrderID:     event.OrderID,
		UserID:      event.UserID,
		Amount:      event.TotalAmount,
		Status:      models.StatusPending,
		Transaction: "",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.Create(ctx, &payment); err != nil {
		return s.handleFailure(ctx, event.OrderID, fmt.Sprintf("failed to create payment: %v", err))
	}

	// Имитация обработки платежа (в реальности здесь был бы вызов платёжного шлюза)
	transactionID := s.processPayment(&payment)

	if transactionID == "" {
		// Платёж не прошёл
		_ = s.repo.UpdateStatus(ctx, payment.ID, models.StatusFailed, "")
		return s.handleFailure(ctx, event.OrderID, "payment processing failed")
	}

	if err := s.repo.UpdateStatus(ctx, payment.ID, models.StatusProcessed, transactionID); err != nil {
		s.logger.Errorf("Failed to update payment status: %v", err)
	}

	succesEvent := models.OrderPaymentProcessed{
		OrderID:     event.OrderID,
		Transaction: transactionID}

	if err := s.publishEvent(ctx, "PaymentProcessed", succesEvent); err != nil {
		s.logger.Errorf("Failed to publish PaymentProcessed event: %v", err)
		return err
	}

	s.logger.Infof("Payment processed successfully for order %s, transaction: %s", event.OrderID, transactionID)
	return nil
}

func (s *PaymentService) processPayment(payment *models.Payment) string {
	// В реальности здесь был бы вызов платёжного шлюза (Stripe, YooKassa и т.д.)
	transactionID := fmt.Sprintf("TXN-%s", uuid.New().String()[:8])

	s.logger.Infof("Processing payment: order=%s, amount=%.2f, transaction=%s",
		payment.OrderID, payment.Amount, transactionID)

	// Имитация задержки обработки
	time.Sleep(100 * time.Millisecond)

	return transactionID
}

func (s *PaymentService) handleFailure(ctx context.Context, orderID uuid.UUID, reason string) error {
	s.logger.Warnf("Payment failed for order %s: %s", orderID, reason)

	failedEvent := models.OrderPaymentFailed{
		OrderID: orderID,
		Reason:  reason,
	}

	if err := s.publishEvent(ctx, "PaymentFailed", failedEvent); err != nil {
		s.logger.Errorf("Failed to publish PaymentFailed event: %v", err)
	}

	return fmt.Errorf(reason)
}

func (s *PaymentService) publishEvent(ctx context.Context, eventType string, event interface{}) error {
	if s.paymentProducer == nil {
		s.logger.Errorf("Kafka producer is nil! Event %s will NOT be sent", eventType)
		return fmt.Errorf("Kafka Producer is nil")
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	s.logger.Infof("Publishing event to Kafka:")
	s.logger.Infof("   Type: %s", eventType)
	s.logger.Infof("   Topic: %s", s.topic)
	s.logger.Infof("   Payload: %s", string(eventBytes))

	msg := kafka.Message{
		Topic: s.topic,
		Key:   []byte(eventType),
		Value: eventBytes,
		Time:  time.Now(),
	}

	err = s.paymentProducer.WriteMessages(ctx, msg)

	if err != nil {
		s.logger.Errorf("❌ FAILED to publish to Kafka: %v", err)
		return fmt.Errorf("publish event: %w", err)
	}

	s.logger.Infof("✅ Successfully published event %s to topic %s", eventType, s.topic)
	return nil
}
