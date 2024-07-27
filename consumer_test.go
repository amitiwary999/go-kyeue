package gokyeue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kyeue "github.com/amitiwary999/go-kyeue"
)

type Handler struct{}

func (h *Handler) MessageHandler(msg kyeue.Message) error {
	fmt.Printf("message received %v \n", msg.Id)
	return nil
}
func TestConsumer(t *testing.T) {
	queue, err := kyeue.InitStorage("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", 10, 10)
	if err != nil {
		t.Errorf("failed to create storage %v ", err)
	} else {
		consumer := kyeue.NewQueueConsumer(queue, "test_queue", 1, &Handler{})
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(3*time.Second))
		consumer.Consume(ctx)
	}
}
