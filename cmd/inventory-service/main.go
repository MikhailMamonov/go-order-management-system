package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/consumers"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/handlers"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/repository"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/services"
	"github.com/MikhailMamonov/go-order-management-system/pkg/database"
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

	inventoryRepo := repository.NewInventoryRepository(db)
	inventoryService := services.NewInventoryService(inventoryRepo, nil, viper.GetString("kafka.topics.inventory"), sugar)
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

	go func() {
		sugar.Infof("Inventory service started on port : %s", viper.GetString("server.port"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugar.Fatalf("Failed to start server: %v", err)
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

	sugar.Info("Server exited gracefully")
}
