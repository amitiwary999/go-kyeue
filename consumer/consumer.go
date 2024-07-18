package consumer

import (
	"fmt"
	"time"

	util "github.com/amitiwary999/go-kyeue/util"
)

type queueConsumer struct {
	queue        util.QueueStorgae
	queueName    string
	pollInterval int16
	consumeCount int
	startPoll    chan string
	handle       util.MessageHandle
}

func NewQueueConsumer(queue util.QueueStorgae, queueName string, pollInterval int16, consumeCount int, handle util.MessageHandle) *queueConsumer {
	consumer := &queueConsumer{
		queue:        queue,
		queueName:    queueName,
		pollInterval: pollInterval,
		consumeCount: consumeCount,
		startPoll:    make(chan string),
		handle:       handle,
	}
	consumer.ConsumePrevMessage()
	return consumer
}

func (c *queueConsumer) Consume() {
	<-c.startPoll
}

func (c *queueConsumer) ConsumePrevMessage() {
	msgs, err := c.queue.ReadPrevMessageOnLoad(c.consumeCount, time.Now(), c.queueName)
	if err != nil {
		fmt.Printf("failed to read prev message on %v ", time.Now())
		c.startPoll <- "0"
	} else {
		latestMsgId := msgs[0].Id
		c.startPoll <- latestMsgId

	}
}
