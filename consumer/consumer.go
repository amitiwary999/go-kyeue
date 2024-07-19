package consumer

import (
	"fmt"
	"time"

	util "github.com/amitiwary999/go-kyeue/util"
)

type queueConsumer struct {
	queue        util.QueueStorgae
	queueName    string
	consumeCount int
	startPoll    chan string
	handle       util.MessageHandle
}

func NewQueueConsumer(queue util.QueueStorgae, queueName string, consumeCount int, handle util.MessageHandle) *queueConsumer {
	consumer := &queueConsumer{
		queue:        queue,
		queueName:    queueName,
		consumeCount: consumeCount,
		startPoll:    make(chan string),
		handle:       handle,
	}
	consumer.ConsumePrevMessage()
	return consumer
}

func (c *queueConsumer) Consume() {
	idOffset := <-c.startPoll
	for {
		msgs, err := c.queue.Read(c.consumeCount, idOffset)
		if err != nil {
			fmt.Printf("failed to read the message from queue")
		} else {
			for _, msg := range msgs {
				c.handle.MessageHandler(msg)
			}
		}
	}
}

func (c *queueConsumer) ConsumePrevMessage() {
	msgs, err := c.queue.ReadPrevMessageOnLoad(c.consumeCount, time.Now(), c.queueName)
	if err != nil {
		fmt.Printf("failed to read prev message on %v ", time.Now())
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
