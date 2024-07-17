package consumer

import (
	util "github.com/amitiwary999/go-kyeue/util"
)

type queueConsumer struct {
	queue        util.QueueStorgae
	queueName    string
	pollInterval int16
	startPoll    chan int
	handle       util.MessageHandle
}

func NewQueueConsumer(queue util.QueueStorgae, queueName string, pollInterval int16, handle util.MessageHandle) *queueConsumer {
	return &queueConsumer{
		queue:        queue,
		queueName:    queueName,
		pollInterval: pollInterval,
		startPoll:    make(chan int),
		handle:       handle,
	}
}

func (c *queueConsumer) Consume() {
	<-c.startPoll
}

func (c *queueConsumer) ConsumePrevMessage() {

}
