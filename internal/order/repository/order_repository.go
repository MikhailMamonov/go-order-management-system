package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/MikhailMamonov/go-order-management-system/internal/order/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type OrderRepository interface {
	Create(ctx context.Context, order *models.Order) error
	GetByID(ctx context.Context, id uuid.UUID) (*models.Order, error)
	List(ctx context.Context) ([]models.Order, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.OrderStatus) error
}

type postgresOrderRepository struct {
	db *sqlx.DB
}

func NewOrderRepository(db *sqlx.DB) OrderRepository {
	return &postgresOrderRepository{db: db}
}

func (r *postgresOrderRepository) Create(ctx context.Context, order *models.Order) error {
	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("marshal items: %w", err)
	}

	query := `INSERT INTO orders  (id, user_id, items, total_amount, status, created_at, updated_at) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			`
	_, err = r.db.ExecContext(ctx, query,
		order.ID,
		order.UserID,
		itemsJSON,
		order.TotalAmount,
		order.Status,
		order.CreatedAt,
		order.UpdatedAt)

	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	return nil
}

func (r *postgresOrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Order, error) {
	var order models.Order
	var itemsJSON []byte

	query := `
		SELECT id, user_id, items, total_amount, status, created_at, updated_at
		FROM orders
		WHERE id = $1
	`

	err := r.db.QueryRowxContext(ctx, query, id).Scan(
		&order.ID,
		&order.UserID,
		&itemsJSON,
		&order.TotalAmount,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("order not found")
	}

	if err != nil {
		return nil, fmt.Errorf("query order: %w", err)
	}

	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return nil, fmt.Errorf("unmarshal items: %w", err)
	}

	return &order, nil
}

func (r *postgresOrderRepository) List(ctx context.Context) ([]models.Order, error) {

}
func (r *postgresOrderRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status models.OrderStatus) error
