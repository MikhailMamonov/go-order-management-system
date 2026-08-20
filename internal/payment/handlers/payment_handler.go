package handlers

import (
	"net/http"

	"github.com/MikhailMamonov/go-order-management-system/internal/payment/repository"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type PaymentHandler struct {
	paymentRepo repository.PaymentRepository
}

func NewPaymentHandler(paymentRepo repository.PaymentRepository) *PaymentHandler {
	return &PaymentHandler{paymentRepo: paymentRepo}
}

func (h *PaymentHandler) RegisterRoutes(router *gin.Engine) {
	payments := router.Group("api/v1/payments")
	{
		payments.GET("/order/:order_id", h.GetPaymentByOrderID)
	}
}

func (h *PaymentHandler) GetPaymentByOrderID(c *gin.Context) {
	orderID, err := uuid.Parse(c.Param("order_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}

	payment, err := h.paymentRepo.GetByOrderID(c.Request.Context(), orderID)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, payment)
}
