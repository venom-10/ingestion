package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectDB(ctx context.Context) *pgxpool.Pool {
	dbUrl := os.Getenv("DATABASE_URL")

	db, err := pgxpool.New(ctx, dbUrl)

	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatal("PostgreSQL connection failed:", err)
	}

	fmt.Println("PostgreSQL connected!")
	return db;
}
