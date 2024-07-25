package gokyeue

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
type QueueStorgae interface {
	CreateChannel(string) error
	CreateDeadLetterQueue(string) error
	Save(string, []byte, string) error
	Read(int, string, string) ([]Message, error)
}

type MessageHandle interface {
	MessageHandler(Message) error
}
