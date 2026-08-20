package models

import (
	"time"

	"github.com/google/uuid"
)

type PaymentStatus string

const (
	StatusPending   PaymentStatus = "PENDING"
	StatusProcessed PaymentStatus = "PROCESSED"
	StatusFailed    PaymentStatus = "FAILED"
)

type Payment struct {
	ID          uuid.UUID     `json:"id" db:"id"`
	OrderID     uuid.UUID     `json:"order_id" db:"order_id"`
	UserID      uuid.UUID     `json:"user_id" db:"user_id"`
	Amount      float64       `json:"amount" db:"amount"`
	Status      PaymentStatus `json:"status" db:"status"`
	Transaction string        `json:"transaction" db:"transaction"`
	CreatedAt   time.Time     `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at" db:"updated_at"`
}

type OrderPendingEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	UserID      uuid.UUID `json:"user_id"`
	TotalAmount float64   `json:"total_amount"`
}

type OrderPaymentProcessed struct {
	OrderID     uuid.UUID `json:"order_id"`
	Transaction string    `json:"transaction"`
}

type OrderPaymentFailed struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}
