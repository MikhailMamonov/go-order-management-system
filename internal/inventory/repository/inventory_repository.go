package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type InventoryRepository interface {
	GetByProductID(ctx context.Context, productID uuid.UUID) (*models.Inventory, error)
	List(ctx context.Context) ([]models.Inventory, error)
	Create(ctx context.Context, inventory *models.Inventory) error
	UpdateQuantity(ctx context.Context, productID uuid.UUID, quantity int) error
	Reserve(ctx context.Context, reservations map[uuid.UUID]int) error
	Release(ctx context.Context, reservations map[uuid.UUID]int) error
}

type postgresInventoryRepository struct {
	db *sqlx.DB
}

func NewInventoryRepository(db *sqlx.DB) InventoryRepository {
	return &postgresInventoryRepository{db: db}
}

func (r *postgresInventoryRepository) GetByProductID(ctx context.Context, productID uuid.UUID) (*models.Inventory, error) {
	var inventory models.Inventory
	query := `
	SELECT  product_id, name, quantity, price, reserved_quantity, updated_at
	FROM inventory
	WHERE product_id=$1
	`

	err := r.db.QueryRowxContext(ctx, query, productID).Scan(
		&inventory.ProductID,
		&inventory.Name,
		&inventory.Quantity,
		&inventory.Price,
		&inventory.ReservedQuantity,
		&inventory.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("product not found")
	}

	if err != nil {
		return nil, fmt.Errorf("query inventory: %w", err)
	}

	return &inventory, nil
}

func (r *postgresInventoryRepository) List(ctx context.Context) ([]models.Inventory, error) {
	var inventories []models.Inventory
	query := `
	SELECT product_id, name, quantity, price, reserved_quantity, updated_at
	FROM inventory
	`
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query inventory list: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var inventory models.Inventory
		if err := rows.Scan(&inventory.ProductID, &inventory.Name, &inventory.Quantity, &inventory.Price,
			&inventory.ReservedQuantity, &inventory.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan inventory: %w", err)
		}
		inventories = append(inventories, inventory)
	}

	return inventories, nil
}

func (r *postgresInventoryRepository) Create(ctx context.Context, inventory *models.Inventory) error {
	query := `
		INSERT INTO inventory (product_id, name, quantity, price, reserved_quantity, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (product_id) DO UPDATE
		SET name = EXCLUDED.name, quantity = EXCLUDED.quantity, price = EXCLUDED.price, updated_at = EXCLUDED.updated_at
	`
	_, err := r.db.ExecContext(ctx, query,
		inventory.ProductID,
		inventory.Name,
		inventory.Quantity,
		inventory.Price,
		inventory.ReservedQuantity,
		inventory.UpdatedAt)

	return err
}

func (r *postgresInventoryRepository) UpdateQuantity(ctx context.Context, productID uuid.UUID, quantity int) error {
	query := `
		UPDATE inventory
		SET quantity = $1, updated_at = NOW()
		WHERE product_id = $2
	`
	_, err := r.db.ExecContext(ctx, query, quantity, productID)
	return err
}

func (r *postgresInventoryRepository) Reserve(ctx context.Context, reservations map[uuid.UUID]int) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer tx.Rollback()

	for productID, quantity := range reservations {
		var available int
		query := `
			SELECT (quantity - reserved_quantity) 
			FROM inventory 
			WHERE product_id = $1
			FOR UPDATE
		`
		err := tx.QueryRowxContext(ctx, query, productID).Scan(&available)
		if err == sql.ErrNoRows {
			return fmt.Errorf("product %s not found", productID)
		}
		if err != nil {
			return fmt.Errorf("query product: %w", err)
		}

		if available < quantity {
			return fmt.Errorf("insufficient quantity for product %s: available=%d, requested=%d",
				productID, available, quantity)
		}

		updateQuery := `
			UPDATE inventory
			SET reserved_quantity = reserved_quantity+$1, updated_at = NOW()
			WHERE product_id = $2
		`
		if _, err := tx.ExecContext(ctx, updateQuery, quantity, productID); err != nil {
			return fmt.Errorf("update reserved: %w", err)
		}
	}

	return tx.Commit()
}

func (r *postgresInventoryRepository) Release(ctx context.Context, reservations map[uuid.UUID]int) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer tx.Rollback()

	for productID, quantity := range reservations {
		updateQuery := `
			UPDATE inventory
			SET reserved_quantity = GREATEST(0,reserved_quantity - $1), updated_at = NOW()
			WHERE product_id = $2
		`
		if _, err := tx.ExecContext(ctx, updateQuery, quantity, productID); err != nil {
			return fmt.Errorf("release reserved: %w", err)
		}
	}

	return tx.Commit()
}
