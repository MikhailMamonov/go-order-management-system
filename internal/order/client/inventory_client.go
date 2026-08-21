package client

import (
	"context"
	"fmt"

	pb "github.com/MikhailMamonov/go-order-management-system/api/inventory/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type InventoryGRPCClient struct {
	conn   *grpc.ClientConn
	client pb.InventoryServiceClient
}

func NewInventoryGRPCClient(address string) (*InventoryGRPCClient, error) {

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return &InventoryGRPCClient{
		conn:   conn,
		client: pb.NewInventoryServiceClient(conn),
	}, nil
}

func (c *InventoryGRPCClient) CheckStock(ctx context.Context, productID string) (int, error) {
	resp, err := c.client.CheckStock(ctx, &pb.CheckStockRequest{
		ProductId: productID,
	})

	if err != nil {
		return 0, err
	}

	return int(resp.Quantity), nil

}

func (c *InventoryGRPCClient) ReserveStock(ctx context.Context, productID string, quantity int, orderID string) (string, error) {
	resp, err := c.client.ReserveStock(ctx, &pb.ReserveStockRequest{
		ProductId: productID,
		Quantity:  int32(quantity),
		OrderId:   orderID,
	})

	if err != nil {
		return "", err
	}

	if !resp.Success {
		return "", fmt.Errorf("failed to reserve stock")
	}

	return resp.ReservationId, nil
}

func (c *InventoryGRPCClient) Close() error {
	return &c.conn.Close()
}
