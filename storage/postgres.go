package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	model "github.com/amitiwary999/go-kyeue/model"
)

type PostgresDbClient struct {
	DB        *sql.DB
	timeout   int16
	dataLimit int16
}

func NewPostgresClient(connectionUrl string, poolLimit int16, timeout int16, dataLimit int16) (*PostgresDbClient, error) {
	db, err := sql.Open("postgres", connectionUrl)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(int(poolLimit))
	db.SetMaxIdleConns(int(poolLimit))
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return &PostgresDbClient{
		DB:        db,
		timeout:   timeout,
		dataLimit: dataLimit,
	}, nil
}

func (pgdb *PostgresDbClient) Save(id string, payload []byte, queueName string) error {
	query := fmt.Sprintf("INSERT INTO %v(id, payload) VALUES($1, $2)", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	_, err := pgdb.DB.QueryContext(ctx, query, id, payload)
	return err
}

func (pgdb *PostgresDbClient) Read(consumeCountLimit int, lastId string, queueName string) ([]model.Message, error) {
	var msgs []model.Message
	query := fmt.Sprintf("UPDATE %v SET consume_count = consume_count + 1 WHERE consume_count < $1 AND id > $2 LIMIT $3; Returning id, payload, consume_count", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	rows, err := pgdb.DB.QueryContext(ctx, query, consumeCountLimit, lastId, pgdb.dataLimit)
	if err != nil {
		return msgs, nil
	}
	for rows.Next() {
		var msg model.Message
		rows.Scan(&msg.Id, &msg.Payload, &msg.ConsumeCount)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (pgdb *PostgresDbClient) ReadPrevMessageOnLoad(consumeCountLimit int, timestamp string, queueName string) ([]model.Message, error) {
	var msgs []model.Message
	query := fmt.Sprintf("UPDATE %v SET consume_count = consume_count + 1 WHERE consume_count < $1 AND created_at < $2 LIMIT $3; Returning id, payload, consume_count", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	rows, err := pgdb.DB.QueryContext(ctx, query, consumeCountLimit, timestamp, pgdb.dataLimit)
	if err != nil {
		return msgs, nil
	}
	for rows.Next() {
		var msg model.Message
		rows.Scan(&msg.Id, &msg.Payload, &msg.ConsumeCount)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
