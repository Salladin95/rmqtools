package rmqtools

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Emitter struct {
	connection   *amqp.Connection
	exchangeName string
}

func (e *Emitter) Setup() error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	return DeclareExchange(channel, e.exchangeName)
}

func (e *Emitter) Push(ctx context.Context, routingKey string, data []byte) error {
	channel, err := e.connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	log.Println("Pushing to channel")

	err = channel.PublishWithContext(
		ctx,
		e.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func NewEventEmitter(conn *amqp.Connection, exchangeName string) (Emitter, error) {
	emitter := Emitter{
		connection:   conn,
		exchangeName: exchangeName,
	}

	err := emitter.Setup()
	if err != nil {
		return Emitter{}, err
	}

	return emitter, nil
}
