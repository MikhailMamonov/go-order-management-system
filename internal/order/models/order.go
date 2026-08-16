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
