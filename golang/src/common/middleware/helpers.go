package middleware

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handleError(err error, connection *amqp.Connection) error {
	_ = closeConnection(connection)

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}

func connect(connectionSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	connection, err := amqp.Dial(url)

	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}
	channel, err := connection.Channel()

	if err != nil {
		return nil, nil, handleError(err, connection)
	}
	return connection, channel, nil
}

func closeConnection(connection *amqp.Connection) error {
	if !connection.IsClosed() {
		if err := connection.Close(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	return nil
}

func declareQueue(channel *amqp.Channel, queueName string, durable bool, exclusive bool) (amqp.Queue, error) {
	return channel.QueueDeclare(
		queueName,
		durable,
		false,
		exclusive,
		false,
		nil,
	)
}

func getConsumeChannel(channel *amqp.Channel, queueName string, consumer string) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queueName,
		consumer,
		false,
		false,
		false,
		false,
		nil,
	)
}

func ack(delivery amqp.Delivery) {
	_ = delivery.Ack(false)
}

func nack(delivery amqp.Delivery) {
	_ = delivery.Nack(false, true)
}

func consumeDeliveries(deliveries <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	for delivery := range deliveries {
		message := Message{
			Body: string(delivery.Body),
		}
		callbackFunc(
			message,
			func() { ack(delivery) },
			func() { nack(delivery) },
		)
	}
}

func stopConsuming(channel *amqp.Channel, consumer string) error {
	return channel.Cancel(consumer, false)
}

func createPublishing(message Message) amqp.Publishing {
	return amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(message.Body),
	}
}

func publish(channel *amqp.Channel, exchange string, key string, publishing amqp.Publishing) error {
	return channel.Publish(
		exchange,
		key,
		false,
		false,
		publishing,
	)
}
