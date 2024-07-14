package sqlx

import (
	"time"

	"github.com/jmoiron/sqlx"
)

type ConnectionOpt func(*sqlx.DB)

func WithMaxOpenConns(n int) ConnectionOpt {
	return func(c *sqlx.DB) {
		c.DB.SetMaxOpenConns(n)
	}
}

func WithMaxIdleConns(n int) ConnectionOpt {
	return func(c *sqlx.DB) {
		c.DB.SetMaxIdleConns(n)
	}
}

func WithConnMaxLifetime(d time.Duration) ConnectionOpt {
	return func(c *sqlx.DB) {
		c.DB.SetConnMaxLifetime(d)
	}
}

func WithMaxIdleTime(d time.Duration) ConnectionOpt {
	return func(c *sqlx.DB) {
		c.DB.SetConnMaxIdleTime(d)
	}
}

func NewConnection(driver, uri string, opts ...ConnectionOpt) (*sqlx.DB, error) {
	conn, err := sqlx.Open(driver, uri)
	if err != nil {
		panic(err)
	}

	for _, opt := range opts {
		opt(conn)
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return conn, nil
}
