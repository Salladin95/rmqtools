package event

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// DeclareExchange creates or declares a topic exchange on the given channel.
func DeclareExchange(ch *amqp.Channel, exchangeName string) error {
	return ch.ExchangeDeclare(
		exchangeName, // name of the exchange
		"topic",      // type of the exchange (topic)
		true,         // durable: the exchange survives broker restarts
		false,        // auto-deleted: the exchange is not deleted when there are no more queues bound to it
		false,        // internal: this is not an internal exchange used by other exchanges
		false,        // no-wait: do not wait for the server to confirm the exchange creation
		nil,          // arguments: additional settings for the exchange (none in this case)
	)
}

// DeclareRandomQueue creates or declares an anonymous exclusive queue on the given channel.
func DeclareRandomQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		"",    // name: a unique name is generated by the server (anonymous queue)
		false, // durable: the queue will not survive broker restarts
		false, // delete when unused: the queue will be deleted when there are no more consumers or bindings
		true,  // exclusive: the queue can only be used by the connection that created it
		false, // no-wait: do not wait for the server to confirm the queue creation
		nil,   // arguments: additional settings for the queue (none in this case)
	)
}
