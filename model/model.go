package model

import "encoding/json"

type Message struct {
	Id           string
	Payload      json.RawMessage
	ConsumeCount int
}
