package util

import "encoding/json"

type Message struct {
	Id           string
	Payload      json.RawMessage
	ConsumeCount int
}

type QueueStorgae interface {
	Save(string, []byte, string) error
	Read() []Message
	ReadWithOffset(string) []Message
}
