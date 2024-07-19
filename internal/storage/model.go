package storage

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
