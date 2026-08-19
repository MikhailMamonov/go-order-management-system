package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MikhailMamonov/go-order-management-system/internal/order/consumers"
	"github.com/MikhailMamonov/go-order-management-system/internal/order/handlers"
	"github.com/MikhailMamonov/go-order-management-system/internal/order/repository"
	"github.com/MikhailMamonov/go-order-management-system/internal/order/services"
	"github.com/MikhailMamonov/go-order-management-system/pkg/config"
	"github.com/MikhailMamonov/go-order-management-system/pkg/database"
	"github.com/MikhailMamonov/go-order-management-system/pkg/kafka"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	sugar := logger.Sugar()

	cfg, err := config.Load()

	if err != nil {
		sugar.Fatalf("Failed to load config: %v", err)
	}

	db, err := database.NewPostgresDB(database.PostgresConfig{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.DBName,
		SSLMode:  cfg.Database.SSLMode,
	})

	if err != nil {
		sugar.Fatalf("Failed to connect to database: %v", err)
	}

	defer db.Close()
	kafkaProducer, err := kafka.NewProducer(cfg.Kafka.Brokers)

	if err != nil {
		sugar.Fatalf("Failed to create Kafka producer: %v", err)
	}

	defer kafkaProducer.Close()

	// Dependency Injection
	orderRepo := repository.NewOrderRepository(db)
	orderService := services.NewOrderService(orderRepo, kafkaProducer, cfg.Kafka.Topics.Orders, sugar)
	orderHandler := handlers.NewOrderHandler(orderService)

	inventoryConsumer := consumers.NewInventoryConsumer(
		viper.GetStringSlice("kafka.brokers"),
		orderService,
		sugar,
	)

	// Настройка роутера
	router := gin.Default()
	orderHandler.RegisterRoutes(router)

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Server.Port),
		Handler: router,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := inventoryConsumer.Start(ctx); err != nil {
			sugar.Errorf("Inventory consumer error: %v", err)
		}
	}()

	go func() {
		sugar.Infof("Order Service starting on port %s", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugar.Fatalf("Failed to start server: %v", err)
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
		sugar.Fatalf("Server forced to shutdown: %v", err)
	}

	sugar.Info("Server exited gracefully")
}
