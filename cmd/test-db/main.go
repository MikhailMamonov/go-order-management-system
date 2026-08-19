package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	dsn := "host=localhost port=5432 user=postgres password=postgres dbname=order_db sslmode=disable"
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect :%v", err)
	}

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}

	fmt.Println("✅ Successfully connected to Order DB")

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&count)

	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}

	fmt.Printf("Orders count: %d", count)
}
