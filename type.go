package gokyeue

import (
	"time"

	storage "github.com/amitiwary999/go-kyeue/internal/storage"
)

type QueueStorgae interface {
	CreateChannel(string) error
	Save(string, []byte, string) error
	Read(int, string) ([]storage.Message, error)
	ReadPrevMessageOnLoad(int, time.Time, string) ([]storage.Message, error)
}

type MessageHandle interface {
	MessageHandler(storage.Message) error
}
