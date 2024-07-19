package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
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

func (pgdb *PostgresDbClient) CreateChannel(queueName string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		id             string                                NOT NULL PRIMARY KEY,
	    payload        JSONB                                 NOT NULL,
		consume_count  INTEGER     DEFAULT 0                 NOT NULL,
		created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
	);
	CREATE INDEX IF NOT EXISTS "%s_created_at_idx" ON %s (created_at);
	`, queueName, queueName, queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	_, err := pgdb.DB.QueryContext(ctx, query)
	return err
}

func (pgdb *PostgresDbClient) Save(id string, payload []byte, queueName string) error {
	query := fmt.Sprintf("INSERT INTO %v(id, payload) VALUES($1, $2)", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	_, err := pgdb.DB.QueryContext(ctx, query, id, payload)
	return err
}

func (pgdb *PostgresDbClient) Read(consumeCountLimit int, lastId string, queueName string) ([]Message, error) {
	var msgs []Message
	query := fmt.Sprintf("UPDATE %v SET consume_count = consume_count + 1 WHERE consume_count < $1 AND id > $2 LIMIT $3; Returning id, payload, consume_count, created_at", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	rows, err := pgdb.DB.QueryContext(ctx, query, consumeCountLimit, lastId, pgdb.dataLimit)
	if err != nil {
		return msgs, nil
	}
	for rows.Next() {
		var msg Message
		rows.Scan(&msg.Id, &msg.Payload, &msg.ConsumeCount, &msg.CreatedAt)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (pgdb *PostgresDbClient) ReadPrevMessageOnLoad(consumeCountLimit int, timeStamp time.Time, queueName string) ([]Message, error) {
	var msgs []Message
	query := fmt.Sprintf("UPDATE %v SET consume_count = consume_count + 1 WHERE consume_count < $1 AND created_at <= $2 ORDER BY created_at DESC LIMIT $3; Returning id, payload, consume_count, created_at", queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	rows, err := pgdb.DB.QueryContext(ctx, query, consumeCountLimit, timeStamp, pgdb.dataLimit)
	if err != nil {
		return msgs, nil
	}
	for rows.Next() {
		var msg Message
		rows.Scan(&msg.Id, &msg.Payload, &msg.ConsumeCount, &msg.CreatedAt)
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
