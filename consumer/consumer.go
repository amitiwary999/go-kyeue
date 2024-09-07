package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/amitiwary999/go-kyeue/model"
	"github.com/amitiwary999/go-kyeue/util"
	"golang.org/x/sync/semaphore"
)

var mu sync.Mutex

type queueConsumer struct {
	queue             model.QueueStorgae
	queueName         string
	consumeCount      int
	handle            model.MessageHandle
	deadLetterQueue   string
	weightedSem       *semaphore.Weighted
	messageFetchLimit int64
}

func NewQueueConsumer(queue model.QueueStorgae, queueName string, consumeCount int, handle model.MessageHandle) *queueConsumer {
	consumer := &queueConsumer{
		queue:             queue,
		queueName:         queueName,
		consumeCount:      consumeCount,
		handle:            handle,
		messageFetchLimit: 1000,
	}
	return consumer
}

func (c *queueConsumer) SetLimit(weightLimit, fetchLimit int64) {
	sem := semaphore.NewWeighted(int64(weightLimit))
	c.weightedSem = sem
	c.messageFetchLimit = fetchLimit
}

func (c *queueConsumer) blockWeight(ctx context.Context, weight int64) (int64, error) {
	if c.weightedSem == nil {
		return weight, nil
	}
	for i := weight; i > 1; i = i - 10 {
		if ok := c.weightedSem.TryAcquire(i); ok {
			return i, nil
		}
	}
	if err := c.weightedSem.Acquire(ctx, 10); err != nil {
		return 0, err
	}
	return 10, nil
}

func (c *queueConsumer) AddDeadLetterQueue() {
	const deadLetterQueue = `gokyeue_dead_letter_queue`
	err := c.queue.CreateDeadLetterQueue(deadLetterQueue)
	if err == nil {
		c.deadLetterQueue = deadLetterQueue
	} else {
		fmt.Printf("failed to created dead letter queue, error %v \n", err)
	}
}

func (c *queueConsumer) handleMessage(msg model.Message) {
	defer c.weightedSem.Release(1)
	err := c.handle.MessageHandler(msg)
	if err != nil {
		fmt.Printf("failed to process message %v \n", err)
		err = c.queue.SaveDeadLetterQueue(c.queueName, msg, err.Error())
		if err != nil {
			fmt.Printf("failed to save in dead letter queue msg id %s \n", msg.Id)
		}
	}
}

func (c *queueConsumer) Consume(ctx context.Context) error {
	idOffset := "0"
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			limit, err := c.blockWeight(ctx, c.messageFetchLimit)
			if err != nil {
				return err
			}
			msgs, err := c.queue.Read(c.consumeCount, limit, idOffset, c.queueName)
			if err != nil {
				if util.IsPgNonRecoveredError(err) {
					return err
				}
				fmt.Printf("failed to read the message from queue %v \n", err)
			} else {
				for _, msg := range msgs {
					go c.handleMessage(msg)
				}
				if len(msgs) > 0 {
					idOffset = msgs[len(msgs)-1].Id
				}
			}
		}
	}
}
