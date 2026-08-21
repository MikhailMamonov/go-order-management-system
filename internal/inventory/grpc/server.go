package grpc

import (
	"context"

	pb "github.com/MikhailMamonov/go-order-management-system/api/inventory/v1"
	"github.com/google/uuid"

	// Импортируйте ваш существующий сервис.
	// Замените "services" на точный путь к вашему пакету services, если он называется иначе.
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/services"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InventoryGRPCServer struct {
	pb.UnimplementedInventoryServiceServer
	service *services.InventoryService
}

func NewInventoryServer(service *services.InventoryService) *InventoryGRPCServer {
	return &InventoryGRPCServer{service: service}
}

func (s *InventoryGRPCServer) CheckStock(ctx context.Context, req *pb.CheckStockRequest) (*pb.CheckStockResponse, error) {
	product_id, err := uuid.Parse(req.ProductId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid product_id format: %v", err)
	}

	inventory, err := s.service.GetInventory(ctx, product_id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "inventory not found for product %s: %v", req.ProductId, err)
	}

	availableQty := inventory.AvailableQuantity()

	return &pb.CheckStockResponse{
		Available: availableQty > 0,
		Quantity:  int32(availableQty),
	}, nil
}

func (s *InventoryGRPCServer) ReserveStock(ctx context.Context, req *pb.ReserveStockRequest) (*pb.ReserveStockResponse, error) {
	product_id, err := uuid.Parse(req.ProductId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid product_id format: %v", err)
	}

	order_id, err := uuid.Parse(req.OrderId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid order_id format: %v", err)
	}

	event := models.OrderCreatedEvent{
		OrderID:     order_id,
		TotalAmount: 0,
		UserID:      uuid.Nil,
		Items: []models.OrderItem{
			{ProductID: product_id,
				Quantity: int(req.Quantity)},
		},
	}

	err = s.service.HandleOrderCreated(ctx, event)

	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "reservation failed: %v", err)

	}

	return &pb.ReserveStockResponse{
		Success:       true,
		ReservationId: uuid.New().String(),
	}, nil
}

func (s *InventoryGRPCServer) ReleaseStock(ctx context.Context, req *pb.ReleaseStockRequest) (*pb.ReleaseStockResponse, error) {
	return &pb.ReleaseStockResponse{Success: true}, nil
}
