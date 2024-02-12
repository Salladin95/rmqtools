package rmqtools

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
)

// messageBroker represents a message broker for RabbitMQ.
type messageBroker struct {
	rabbitConn   *amqp091.Connection // RabbitMQ connection
	exchangeName string              // Name of the exchange
	queueName    string              // Name of the queue
}

// MessageBroker defines the interface for interacting with the message broker.
type MessageBroker interface {
	// PushToQueue pushes data to a named queue in RabbitMQ using an EventEmitter.
	// It takes a context, the routingKey, and the data to be pushed.
	// It returns an error if any occurs.
	PushToQueue(ctx context.Context, routingKey string, data interface{}) error

	// ListenForUpdates sets up a consumer to listen for updates on the specified topics.
	// It takes a list of topics to listen to and a message handler function.
	ListenForUpdates(topics []string, mh func(routingKey string, payload []byte))
}

// NewMessageBroker creates a new instance of MessageBroker.
func NewMessageBroker(rabbitConn *amqp091.Connection, exchangeName, queueName string) MessageBroker {
	return &messageBroker{rabbitConn: rabbitConn, exchangeName: exchangeName, queueName: queueName}
}

// PushToQueue pushes data to a named queue in RabbitMQ using an EventEmitter.
// It takes a context, the routingKey, and the data to be pushed.
// It returns an error if any occurs.
func (mb *messageBroker) PushToQueue(ctx context.Context, routingKey string, data interface{}) error {
	// Create a new EventEmitter for the specified AMQP exchange.
	emitter, err := NewEventEmitter(mb.rabbitConn, mb.queueName)
	if err != nil {
		return err
	}

	// Marshal the data into JSON format.
	mData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Push the data to the named queue using the EventEmitter.
	err = emitter.Push(ctx, routingKey, mData)
	if err != nil {
		return err
	}

	return nil
}

// ListenForUpdates sets up a consumer to listen for updates on the specified topics.
// It takes a list of topics to listen to and a message handler function.
func (mb *messageBroker) ListenForUpdates(topics []string, mh func(routingKey string, payload []byte)) {
	// Create a new consumer for the specified AMQP exchange and queue.
	consumer, err := NewConsumer(
		mb.rabbitConn,
		mb.exchangeName,
		mb.queueName,
	)
	if err != nil {
		fmt.Println(err)
	}

	// Start listening for messages on the specified topics and invoke the message handler.
	err = consumer.Listen(topics, mh)
	if err != nil {
		fmt.Println(err)
	}
}
