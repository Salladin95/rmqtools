package emitter

import (
	"github.com/Salladin95/rmqtools/event"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Emitter struct {
	connection   *amqp.Connection
	exchangeName string
	topicName    string
}

func (e *Emitter) Setup() error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}

	defer channel.Close()
	return event.DeclareExchange(channel, e.exchangeName)
}

func (e *Emitter) Push(routingKey, data string) error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	log.Println("Pushing to channel")

	err = channel.Publish(
		e.topicName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func NewEventEmitter(conn *amqp.Connection, exchangeName, topicName string) (Emitter, error) {
	emitter := Emitter{
		connection:   conn,
		exchangeName: exchangeName,
		topicName:    topicName,
	}

	err := emitter.Setup()
	if err != nil {
		return Emitter{}, err
	}

	return emitter, nil
}
