package models

import (
	"time"

	"github.com/google/uuid"
)

type Inventory struct {
	ProductID        uuid.UUID `json:"product_id" db:"product_id"`
	Name             string    `json:"name" db:"name"`
	Quantity         int       `json:"quantity" db:"quantity"`
	Price            float64   `json:"price" db:"price"`
	ReservedQuantity int       `json:"reserved_quantity" db:"reserved_quantity"`
	UpdatedAt        time.Time `json:"updated_at" db:"updated_at"`
}

func (i *Inventory) AvailableQuantity() int {
	return i.Quantity - i.ReservedQuantity
}

type OrderCreatedEvent struct {
	OrderID     uuid.UUID   `json:"order_id"`
	UserID      uuid.UUID   `json:"user_id"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"total_amount"`
}

type OrderCancelledEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}

type InventoryReservedEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	UserID      uuid.UUID `json:"user_id"`
	TotalAmount float64   `json:"total_amount"`
}

type InventoryFailedEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Reason  string    `json:"reason"`
}

type OrderItem struct {
	ProductID uuid.UUID `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Price     float64   `json:"price"`
}

// REST API запросы
type CreateInventoryRequest struct {
	ProductID uuid.UUID `json:"product_id" validate:"required"`
	Name      string    `json:"name" validate:"required"`
	Quantity  int       `json:"quantity" validate:"required,gte=0"`
	Price     float64   `json:"price" validate:"required,gt=0"`
}

type UpdateQuantityRequest struct {
	Quantity int `json:"quantity" validate:"required,gte=0"`
}
