package sqlx

import (
	"time"

	"github.com/jmoiron/sqlx"
)

type Connection = sqlx.DB

type ConnectionOpt func(*Connection)

func WithMaxOpenConns(n int) ConnectionOpt {
	return func(c *Connection) {
		c.DB.SetMaxOpenConns(n)
	}
}

func WithMaxIdleConns(n int) ConnectionOpt {
	return func(c *Connection) {
		c.DB.SetMaxIdleConns(n)
	}
}

func WithConnMaxLifetime(d time.Duration) ConnectionOpt {
	return func(c *Connection) {
		c.DB.SetConnMaxLifetime(d)
	}
}

func WithMaxIdleTime(d time.Duration) ConnectionOpt {
	return func(c *Connection) {
		c.DB.SetConnMaxIdleTime(d)
	}
}

func NewConnection(driver, uri string, opts ...ConnectionOpt) (*Connection, error) {
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
