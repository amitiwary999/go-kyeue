package gokyeue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type PostgresDbClient struct {
	DB      *sql.DB
	timeout int16
}

func newPostgresClient(connectionUrl string, poolLimit int16, timeout int16) (*PostgresDbClient, error) {
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
		DB:      db,
		timeout: timeout,
	}, nil
}

func (pgdb *PostgresDbClient) CreateChannel(queueName string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		id             varchar(50)                           NOT NULL PRIMARY KEY,
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

func (pgdb *PostgresDbClient) CreateDeadLetterQueue(queueName string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	  id              UUID            DEFAULT gen_random_uuid()        NOT NULL PRIMARY KEY,
	  queue_name      varchar(75)                                  NOT NULL,
	  message_id      varchar(50)                                  NOT NULL,
	  payload         JSONB                                        NOT NULL,
	  attemp_count    INTEGER         DEFAULT 0                    NOT NULL,
	  error           TEXT,
	  created_at      TIMESTAMPTZ    DEFAULT CURRENT_TIMESTAMP    NOT NULL        
	);
	CREATE UNIQUE INDEX IF NOT EXISTS "%s_queue_name_message_id_unique_idx" ON %s (queue_name, message_id);
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

func (pgdb *PostgresDbClient) Read(consumeCountLimit int, limit int64, lastId string, queueName string) ([]Message, error) {
	var msgs []Message
	query := fmt.Sprintf("UPDATE %v SET consume_count = consume_count + 1 WHERE id in (SELECT id FROM %v WHERE consume_count < $1 AND id > $2 AND created_at >= NOW() - INTERVAL '7 days' LIMIT $3) Returning id, payload, consume_count, created_at", queueName, queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	rows, err := pgdb.DB.QueryContext(ctx, query, consumeCountLimit, lastId, limit)
	if err != nil {
		return msgs, err
	}
	for rows.Next() {
		var msg Message
		err = rows.Scan(&msg.Id, &msg.Payload, &msg.ConsumeCount, &msg.CreatedAt)
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (pgdb *PostgresDbClient) SaveDeadLetterQueue(queueName string, msg Message, errMsg string) error {
	query := fmt.Sprintf("INSERT INTO %s(queue_name, message_id, payload, error) VALUES($1, $2, $3, $4) ON CONFLICT (queue_name, message_id) DO UPDATE SET error = EXCLUDED.error, attemp_count = %s.attemp_count + 1", queueName, queueName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgdb.timeout)*time.Second)
	defer cancel()
	_, err := pgdb.DB.QueryContext(ctx, query, queueName, msg.Id, msg.Payload, errMsg)
	return err
}
