package util

import "encoding/json"

type Message struct {
	Id           string
	Payload      json.RawMessage
	ConsumeCount int
}

type QueueStorgae interface {
	Send([]byte)
	Read() []Message
	ReadWithOffset(string) []Message
}
