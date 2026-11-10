package main

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_archive.sql
var schema string

func ConnectDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	if url == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func findBlocks(ctx context.Context, db *pgxpool.Pool, station int64, day string) ([]string, error) {
	rows, err := db.Query(ctx, `SELECT object_key FROM archive_blocks WHERE group_id=$1 AND day=$2::date ORDER BY object_key`, int16(makeGroup(station)), day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
