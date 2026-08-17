package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

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
	var orders []models.Order
	var itemsJSON []byte

	query := `
		SELECT id, user_id, items, total_amount, status, created_at, updated_at
		FROM orders
		ORDER BY created_at DESC
	`

	rows, err := r.db.QueryxContext(ctx, query)

	if err != nil {
		return nil, fmt.Errorf("query orders: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var order models.Order
		if err := rows.Scan(
			&order.ID,
			&order.UserID,
			&itemsJSON,
			&order.TotalAmount,
			&order.Status,
			&order.CreatedAt,
			&order.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}

		if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
			return nil, fmt.Errorf("unmarshal items: %w", err)
		}

		orders = append(orders, order)
	}

	return orders, nil
}

func (r *postgresOrderRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status models.OrderStatus) error {
	query := `
		UPDATE orders
		SET status = $1, updated_at = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("order not found")
	}

	return nil
}
