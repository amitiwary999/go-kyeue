package util

import (
	"time"

	model "github.com/amitiwary999/go-kyeue/model"
)

type QueueStorgae interface {
	CreateChannel(string) error
	Save(string, []byte, string) error
	Read(int, string) ([]model.Message, error)
	ReadPrevMessageOnLoad(int, time.Time, string) ([]model.Message, error)
}

type MessageHandle interface {
	MessageHandler(model.Message) error
}
