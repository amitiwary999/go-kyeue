package gokyeue_test

import (
	"fmt"
	"testing"

	kyeue "github.com/amitiwary999/go-kyeue"
)

type Handler struct{}

func (h *Handler) MessageHandler(msg kyeue.Message) error {
	fmt.Printf("message received %v \n", msg.Id)
	return nil
}
func TestConsumer(t *testing.T) {
	queue, err := kyeue.InitStorage("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", 10, 10, 100)
	if err != nil {
		t.Errorf("failed to create storage %v ", err)
	} else {
		consumer := kyeue.NewQueueConsumer(queue, "test_queue", 1, &Handler{})
		consumer.Consume()
	}
}
