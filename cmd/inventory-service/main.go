package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/MikhailMamonov/go-order-management-system/api/inventory/v1"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/consumers"
	inventorygrpc "github.com/MikhailMamonov/go-order-management-system/internal/inventory/grpc"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/handlers"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/repository"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/services"
	"github.com/MikhailMamonov/go-order-management-system/pkg/database"
	"github.com/MikhailMamonov/go-order-management-system/pkg/kafka"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	sugar := logger.Sugar()

	viper.SetConfigName("config-inventory")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	if err := viper.ReadInConfig(); err != nil {
		sugar.Fatalf("Failed to load config %v", err)
	}

	db, err := database.NewPostgresDB(database.PostgresConfig{
		Host:     viper.GetString("database.host"),
		Port:     viper.GetString("database.port"),
		User:     viper.GetString("database.user"),
		Password: viper.GetString("database.password"),
		DBName:   viper.GetString("database.dbname"),
		SSLMode:  viper.GetString("database.sslmode"),
	})

	if err != nil {
		sugar.Fatalf("Failed to connect to database: %v", err)
	}

	defer db.Close()

	kafkaProducer, err := kafka.NewProducer(viper.GetStringSlice("kafka.brokers"))
	if err != nil {
		sugar.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	inventoryRepo := repository.NewInventoryRepository(db)
	inventoryService := services.NewInventoryService(inventoryRepo, kafkaProducer, viper.GetString("kafka.topics.inventory"), sugar)
	inventoryHandler := handlers.NewInventoryHandler(inventoryService)

	router := gin.Default()
	inventoryHandler.RegisterRoutes(router)
	router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	consumer := consumers.NewOrderEventsConsumer(
		viper.GetStringSlice("kafka.brokers"),
		inventoryService,
		sugar,
	)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", viper.GetString("server.port")),
		Handler: router,
	}

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	inventoryServer := inventorygrpc.NewInventoryServer(inventoryService)
	pb.RegisterInventoryServiceServer(grpcServer, inventoryServer)

	go func() {
		sugar.Infof("Inventory service started on port : %s", viper.GetString("server.port"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugar.Fatalf("Failed to start server: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		sugar.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		sugar.Infof("gRPC server listening on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			sugar.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			sugar.Errorf("Consumer error : %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	sugar.Info("Shutting down server...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		sugar.Fatalf("Server forced to shutdown :%v", err)
	}

	sugar.Info("Stopping gRPC server gracefully...")
	grpcServer.GracefulStop()

	sugar.Info("Server exited gracefully")
}
