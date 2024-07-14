package storage

import (
	"context"
	"database/sql"
	"time"

	util "github.com/amitiwary999/go-kyeue/util"
)

type PostgresDbClient struct {
	DB *sql.DB
}

func NewPostgresClient(connectionUrl string, poolLimit int16) (*PostgresDbClient, error) {
	db, err := sql.Open("postgres", connectionUrl)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(int(poolLimit))
	db.SetMaxIdleConns(int(poolLimit))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &PostgresDbClient{
		DB: db,
	}, nil
}

func (pgdb *PostgresDbClient) Save(string, []byte, string) error {
	return nil
}

func (pgdb *PostgresDbClient) Read() []util.Message {
	var msgs []util.Message
	return msgs
}

func (pgdb *PostgresDbClient) ReadWithOffset(string) []util.Message {
	var msgs []util.Message
	return msgs
}
