package producer

import (
	"encoding/json"

	util "github.com/amitiwary999/go-kyeue/util"
	"github.com/oklog/ulid/v2"
)

type queueProducer struct {
	queue util.QueueStorgae
}

func NewQueueProducer(queue util.QueueStorgae) *queueProducer {
	return &queueProducer{
		queue: queue,
	}
}

func (p *queueProducer) CreateChannel(queueName string) error {
	/** TODO custom error handling */
	return p.queue.CreateChannel(queueName)
}

func (p *queueProducer) Send(payload any, queueName string) error {
	bp, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	id := ulid.Make().String()
	return p.queue.Save(id, bp, queueName)
}
