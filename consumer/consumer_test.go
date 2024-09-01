package consumer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/amitiwary999/go-kyeue/consumer"
	"github.com/amitiwary999/go-kyeue/model"
	"github.com/amitiwary999/go-kyeue/storage"
)

type Handler struct{}

func (h *Handler) MessageHandler(msg model.Message) error {
	fmt.Printf("message received %v \n", msg.Id)
	return nil
}
func TestConsumer(t *testing.T) {
	queue, err := storage.NewPostgresClient("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", 10, 10)
	if err != nil {
		t.Errorf("failed to create storage %v ", err)
	} else {
		consumer := consumer.NewQueueConsumer(queue, "test_queue", 1, &Handler{})
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(3*time.Second))
		consumer.Consume(ctx)
	}
}
