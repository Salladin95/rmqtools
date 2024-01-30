package rmqtools

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn         *amqp.Connection
	exchangeName string
	queueName    string
}

func (consumer *Consumer) setup() error {
	channel, err := consumer.conn.Channel()
	if err != nil {
		return err
	}

	return DeclareExchange(channel, consumer.exchangeName)
}

func NewConsumer(conn *amqp.Connection, exchangeName, queueName string) (Consumer, error) {
	consumer := Consumer{
		conn:         conn,
		exchangeName: exchangeName,
		queueName:    queueName,
	}

	err := consumer.setup()
	if err != nil {
		return Consumer{}, err
	}

	return consumer, nil
}

type IncomingMessagesHandler func(routingKey string, payload []byte)

func (consumer *Consumer) Listen(topics []string, messageHandler IncomingMessagesHandler) error {
	ch, err := consumer.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := DeclareRandomQueue(ch)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		ch.QueueBind(
			q.Name,
			topic,
			consumer.exchangeName,
			false,
			nil,
		)

		if err != nil {
			return err
		}
	}

	messages, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	forever := make(chan bool)
	go func() {
		for d := range messages {
			fmt.Printf("calling message handler")
			go messageHandler(d.RoutingKey, d.Body)
		}
	}()

	fmt.Printf("Waiting for message [Exchange, Queue] [logs_topic, %routingKey]\n", q.Name)
	<-forever

	return nil
}
