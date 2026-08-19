CREATE TABLE IF NOT EXISTS inventory (
    product_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    price DECIMAL(10, 2) NOT NULL,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_quantity_positive CHECK (quantity >= 0),
    CONSTRAINT chk_reserved_positive CHECK (reserved_quantity >= 0),
    CONSTRAINT chk_available CHECK (quantity >= reserved_quantity)
);

CREATE INDEX idx_inventory_product_id ON inventory(product_id);

INSERT INTO inventory (product_id, name, quantity, price) VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 'iPhone 15 Pro', 10, 99990.00),
    ('550e8400-e29b-41d4-a716-446655440002', 'MacBook Pro M3', 5, 249990.00),
    ('550e8400-e29b-41d4-a716-446655440003', 'AirPods Pro', 50, 24990.00),
    ('550e8400-e29b-41d4-a716-446655440004', 'iPad Air', 15, 79990.00)
ON CONFLICT (product_id) DO NOTHING;
