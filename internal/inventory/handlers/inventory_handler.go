package handlers

import (
	"net/http"

	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/models"
	"github.com/MikhailMamonov/go-order-management-system/internal/inventory/services"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type InventoryHandler struct {
	inventoryService *services.InventoryService
}

func NewInventoryHandler(inventoryService *services.InventoryService) *InventoryHandler {
	return &InventoryHandler{inventoryService: inventoryService}
}

func (h *InventoryHandler) RegisterRoutes(router *gin.Engine) {
	inventory := router.Group("/api/v1/inventory")
	{
		inventory.POST("", h.CreateInventory)
		inventory.GET("", h.ListInventory)
		inventory.GET("/:product_id", h.GetInventory)
	}
}

func (h *InventoryHandler) CreateInventory(c *gin.Context) {
	var req models.CreateInventoryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	inventory, err := h.inventoryService.CreateInventory(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, inventory)
}

func (h *InventoryHandler) GetInventory(c *gin.Context) {
	productID, err := uuid.Parse(c.Param("product_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid product id"})
		return
	}

	inventory, err := h.inventoryService.GetInventory(c.Request.Context(), productID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, inventory)
}

func (h *InventoryHandler) ListInventory(c *gin.Context) {
	inventories, err := h.inventoryService.ListInventory(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, inventories)
}
