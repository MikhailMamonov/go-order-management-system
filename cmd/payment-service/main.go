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

	"github.com/MikhailMamonov/go-order-management-system/internal/payment/consumers"
	"github.com/MikhailMamonov/go-order-management-system/internal/payment/handlers"
	"github.com/MikhailMamonov/go-order-management-system/internal/payment/repository"
	"github.com/MikhailMamonov/go-order-management-system/internal/payment/services"
	"github.com/MikhailMamonov/go-order-management-system/pkg/database"
	"github.com/MikhailMamonov/go-order-management-system/pkg/kafka"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	sugar := logger.Sugar()

	viper.SetConfigName("config-payments")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		sugar.Fatalf("Failed to load config: %v", err)
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

	paymentRepo := repository.NewPaymentRepository(db)
	paymentService := services.NewPaymentService(paymentRepo, kafkaProducer, viper.GetString("kafka.topics.payments"), sugar)
	paymentHandler := handlers.NewPaymentHandler(paymentRepo)

	router := gin.Default()
	paymentHandler.RegisterRoutes(router)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	consumer := consumers.NewOrderEventsConsumer(viper.GetStringSlice("kafka.brokers"), paymentService, sugar)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", viper.GetString("server.port")),
		Handler: router,
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			sugar.Errorf("Consumer error: %v", err)
		}
	}()

	go func() {
		sugar.Infof("Payment Service starting on port %s", viper.GetString("server.port"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugar.Fatalf("Failed to start server: %v", err)
		}

	}()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	sugar.Info("Shuting down  server ... ")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		sugar.Fatalf("Server forced to shutdown: %v", err)
	}

	sugar.Info("Server exited gracefully")
}
