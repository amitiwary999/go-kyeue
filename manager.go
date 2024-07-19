package gokyeue

import (
	storage "github.com/amitiwary999/go-kyeue/internal/storage"
)

func InitStorage(connectionUrl string, poolLimit int16, timeout int16, dataLimit int16) (*storage.PostgresDbClient, error) {
	return storage.NewPostgresClient(connectionUrl, poolLimit, timeout, dataLimit)
}
