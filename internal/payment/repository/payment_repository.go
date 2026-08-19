package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/MikhailMamonov/go-order-management-system/internal/payment/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type PaymentRepository interface {
	Create(ctx context.Context, payment *models.Payment) error
	GetByOrderID(ctx context.Context, id uuid.UUID) (*models.Payment, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.PaymentStatus, transaction string) error
}

type postgresPaymentRepository struct {
	db *sqlx.DB
}

func NewPaymentRepository(db *sqlx.DB) PaymentRepository {
	return &postgresPaymentRepository{db: db}
}

func (r *postgresPaymentRepository) Create(ctx context.Context, payment *models.Payment) error {
	query := `
		INSERT INTO payments(id, order_id, user_id,amount, status, transaction, created_at, updated_at)
		VALUES ($1,$2, $3,$4, $5, $6, $7, $8)
	`

	_,err := r.db.ExecContext(ctx, query,
		payment.ID,
		payment.OrderID, 
		payment.UserID,
		payment.Amount, 
		payment.Transaction, 
		payment.CreatedAt, 
		payment.UpdatedAt)

	if err!=nil{
		return fmt.Errorf("insert payment: %w", err)
	}
	
	return nil
}


func (r *postgresPaymentRepository) GetByOrderID(ctx context.Context, id uuid.UUID) (*models.Payment, error) {
	var payment models.Payment

	query:= `
		Select id, order_id, user_id,amount, status, transaction, created_at, updated_at 
		from payments
		where order_id= $1
	`

	err := r.db.QueryRowxContext(ctx, query, id).Scan(
		&payment.ID, &payment.OrderID, &payment.UserID, &payment.Amount, &payment.Status, &payment.Transaction, &payment.CreatedAt, &payment.UpdatedAt
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("payment not found")
	}
	if err != nil {
		return nil, fmt.Errorf("query payment: %w", err)
	}

	return &payment, nil
}

func (r *postgresPaymentRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status models.PaymentStatus, transaction string) error {
	query:= `
		Update payments 
		SET status = $1, transaction = $2, updated_at = $3
		where id= $4
	`

	_,err:= r.db.ExecContext(ctx, query, status, transaction, time.Now(), id)

	return err
}
