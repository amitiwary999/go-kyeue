package producer

import (
	util "github.com/amitiwary999/go-kyeue/util"
)

type queueProducer struct {
	queue *util.QueueStorgae
}

func NewQueueProducer(queue *util.QueueStorgae) *queueProducer {
	return &queueProducer{
		queue: queue,
	}
}
