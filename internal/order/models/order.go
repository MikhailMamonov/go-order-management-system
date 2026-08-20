package models

import (
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	StatusPending   OrderStatus = "PENDING"
	StatusReserved  OrderStatus = "RESERVED"
	StatusPaid      OrderStatus = "PAID"
	StatusCancelled OrderStatus = "CANCELLED"
)

type Order struct {
	ID          uuid.UUID   `json:"id" db:"id"`
	UserID      uuid.UUID   `json:"user_id" db:"user_id"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"total_amount" db:"total_amount"`
	Status      OrderStatus `json:"status" db:"status"`
	CreatedAt   time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at" db:"updated_at"`
}

type OrderItem struct {
	ProductID uuid.UUID `json:"product_id" db:"product_id"`
	Quantity  int       `json:"quantity" db:"quantity"`
	Price     float64   `json:"price" db:"price"`
}

type CreateOrderRequest struct {
	UserID uuid.UUID   `json:"user_id" validate:"required"`
	Items  []OrderItem `json:"items" validate:"required,min=1"`
}

// ============ Kafka Events для Saga ============

type OrderCreatedEvent struct {
	OrderID     uuid.UUID   `json:"order_id"`
	UserID      uuid.UUID   `json:"user_id"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"total_amount"`
}

type OrderPendingEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	UserID      uuid.UUID `json:"user_id"`
	TotalAmount float64   `json:"total_amount"`
}

type OrderCancelledEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}

// События от других сервисов (их будет принимать Order Service)

type InventoryReservedEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	UserID      uuid.UUID `json:"user_id"`
	TotalAmount float64   `json:"total_amount"`
}

type InventoryFailedEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}

type PaymentProcessedEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	Transaction string    `json:"transaction"`
}

type PaymentFailedEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}
