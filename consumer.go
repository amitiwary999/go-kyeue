package gokyeue

import (
	"context"
	"fmt"
	"time"
)

type queueConsumer struct {
	queue        QueueStorgae
	queueName    string
	consumeCount int
	startPoll    chan string
	handle       MessageHandle
}

func NewQueueConsumer(queue QueueStorgae, queueName string, consumeCount int, handle MessageHandle) *queueConsumer {
	consumer := &queueConsumer{
		queue:        queue,
		queueName:    queueName,
		consumeCount: consumeCount,
		startPoll:    make(chan string),
		handle:       handle,
	}
	return consumer
}

func (c *queueConsumer) Consume(ctx context.Context) {
	go c.ConsumePrevMessage()
	idOffset := <-c.startPoll
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := c.queue.Read(c.consumeCount, idOffset, c.queueName)
			if err != nil {
				fmt.Printf("failed to read the message from queue %v \n", err)
			} else {
				for _, msg := range msgs {
					c.handle.MessageHandler(msg)
				}
			}
		}
	}
}

func (c *queueConsumer) ConsumePrevMessage() {
	msgs, err := c.queue.ReadPrevMessageOnLoad(c.consumeCount, time.Now(), c.queueName)
	if err != nil {
		fmt.Printf("failed to read prev message on %v err %v", time.Now(), err)
		c.startPoll <- "0"
	} else if len(msgs) == 0 {
		c.startPoll <- "0"
	} else {
		latestMsgId := msgs[0].Id
		c.startPoll <- latestMsgId
		for len(msgs) > 0 {
			timeStampOffset := msgs[0].CreatedAt
			for _, msg := range msgs {
				timeStampOffset = msg.CreatedAt
				c.handle.MessageHandler(msg)
			}
			msgs, err = c.queue.ReadPrevMessageOnLoad(c.consumeCount, timeStampOffset, c.queueName)
			if err != nil {
				fmt.Printf("failed to read prev message on %v ", timeStampOffset)
				break
			}
		}
	}
}
