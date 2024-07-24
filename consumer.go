package gokyeue

import (
	"context"
	"fmt"
)

type queueConsumer struct {
	queue        QueueStorgae
	queueName    string
	consumeCount int
	handle       MessageHandle
}

func NewQueueConsumer(queue QueueStorgae, queueName string, consumeCount int, handle MessageHandle) *queueConsumer {
	consumer := &queueConsumer{
		queue:        queue,
		queueName:    queueName,
		consumeCount: consumeCount,
		handle:       handle,
	}
	return consumer
}

func (c *queueConsumer) Consume(ctx context.Context) error {
	idOffset := "0"
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msgs, err := c.queue.Read(c.consumeCount, idOffset, c.queueName)
			if err != nil {
				if isPgNonRecoveredError(err) {
					return err
				}
				fmt.Printf("failed to read the message from queue %v \n", err)
			} else {
				for _, msg := range msgs {
					c.handle.MessageHandler(msg)
				}
				idOffset = msgs[len(msgs)-1].Id
			}
		}
	}
}
