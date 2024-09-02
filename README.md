# go-kyeue

messaging queue can be use to communicate between the services. There are many messaging queue and all are goods. But sometimes we require simple solution where we don't need to include the more third party library, setup one more system and then maintain it. go-kyeue can be handy in such situation. It use the storage that is currently in use, currently we support postgres storage. 

## To use the library

We need storage first before we start using the producer or consumer. We support postgres only for now

```
import (
    "github.com/amitiwary999/go-kyeue/storage"
)

queue, err := storage.NewPostgresClient("posrtgres url", poollimit, timeout)
```

### Producer
```
import (
    kyeueProducer "github.com/amitiwary999/go-kyeue/producer"
)

producer := kyeueProducer.NewQueueProducer(queue)
err = producer.CreateChannel("queue_name")
payloadStr := "We are sending the test message"
err = producer.Send(payloadStr, "queue_name")
```

### Consumer
```
import (
    kyeueConsumer "github.com/amitiwary999/go-kyeue/consumer"
)

consumer := kyeueConsumer.NewQueueConsumer(queue, "queue_name", consume_count, &Handler{})
ctx, _ := context.WithTimeout(context.Background(), time.Duration(3*time.Second))
consumer.Consume(ctx)
```

### close
Before application close please close the storage connection
```
import (
    "github.com/amitiwary999/go-kyeue/storage"
)

storage.Close()

```
 