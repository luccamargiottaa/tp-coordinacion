package middleware

import amqp "github.com/rabbitmq/amqp091-go"

const prefetchCount = 1

type MessageMiddlewareQueueRabbitMQ struct {
	queueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	consuming  bool
}

func newMiddlewareQueue(queueName string, connection *amqp.Connection, channel *amqp.Channel) (*MessageMiddlewareQueueRabbitMQ, error) {
	queue, err := declareQueue(channel, queueName, true, false)

	if err != nil {
		return nil, handleError(err, connection)
	}
	if err = channel.Qos(prefetchCount, 0, false); err != nil {
		return nil, handleError(err, connection)
	}
	middlewareQueue := MessageMiddlewareQueueRabbitMQ{
		queueName:  queueName,
		connection: connection,
		channel:    channel,
		queue:      queue,
	}
	return &middlewareQueue, nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	middlewareQueue.consuming = true
	deliveries, err := getConsumeChannel(middlewareQueue.channel, middlewareQueue.queueName, middlewareQueue.queueName)

	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	consumeDeliveries(deliveries, callbackFunc)

	if middlewareQueue.consuming {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) StopConsuming() error {
	if !middlewareQueue.consuming {
		return nil
	}
	middlewareQueue.consuming = false
	err := stopConsuming(middlewareQueue.channel, middlewareQueue.queueName)

	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) Send(msg Message) error {
	publishing := createPublishing(msg)

	err := publish(middlewareQueue.channel, "", middlewareQueue.queueName, publishing)

	if err != nil {
		return handleError(err, middlewareQueue.connection)
	}
	return nil
}

func (middlewareQueue *MessageMiddlewareQueueRabbitMQ) Close() error {
	return closeConnection(middlewareQueue.connection)
}
