package model

import (
	"encoding/json"
	"time"
)

type Message struct {
	Id           string
	Payload      json.RawMessage
	ConsumeCount int
	CreatedAt    time.Time
}
type QueueStorage interface {
	CreateChannel(string) error
	CreateDeadLetterQueue(string) error
	SaveDeadLetterQueue(queueName string, msg Message, errMsg string) error
	Save(string, []byte, string) error
	Read(int, int64, string, string) ([]Message, error)
	Close() error
}

type MessageHandle interface {
	MessageHandler(Message) error
}
