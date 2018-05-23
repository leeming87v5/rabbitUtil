package rabbitUtil

import (
	"github.com/streadway/amqp"
)

// Publisher
type PublishCallBack func(confirm amqp.Confirmation, args ...interface{})
type MQPublisher interface {
	Publish(message []byte) (err error)
}

// Consumer
type ConsumeCallBack func(message []byte, args ...interface{}) error
type MQConsumer interface {
	Consume(callback ConsumeCallBack, args ...interface{}) error
}
