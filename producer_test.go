package gokyeue_test

import (
	"testing"

	kyeue "github.com/amitiwary999/go-kyeue"
)

func TestPublisher(t *testing.T) {
	queue, err := kyeue.InitStorage("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", 10, 10, 100)
	if err != nil {
		t.Errorf("failed to create storage %v ", err)
	} else {
		producer := kyeue.NewQueueProducer(queue)
		err = producer.CreateChannel("test_queue")
		if err != nil {
			t.Errorf("failed to created channel %v ", err)
		}
		payloadStr := "We are sending the test message"
		err = producer.Send(payloadStr, "test_queue")
		if err != nil {
			t.Errorf("failed to send message in queue %v ", err)
		}
	}
}
