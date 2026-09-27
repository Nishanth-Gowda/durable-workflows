package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Engine shares a connection pool between API requests and workers.
type Engine struct{ DB *sql.DB }

func New(ctx context.Context, dsn string) (*Engine, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(3 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Engine{DB: db}, nil
}

func (e *Engine) Close() { e.DB.Close() }

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
