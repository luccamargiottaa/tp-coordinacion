package middleware

import amqp "github.com/rabbitmq/amqp091-go"

type MessageMiddlewareExchangeRabbitMQ struct {
	exchange   string
	keys       []string
	connection *amqp.Connection
	channel    *amqp.Channel
	consuming  bool
}

func newMiddlewareExchange(exchange string, keys []string, connection *amqp.Connection, channel *amqp.Channel) (*MessageMiddlewareExchangeRabbitMQ, error) {
	err := channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, handleError(err, connection)
	}
	middlewareExchange := MessageMiddlewareExchangeRabbitMQ{
		exchange:   exchange,
		keys:       keys,
		connection: connection,
		channel:    channel,
	}
	return &middlewareExchange, nil
}

func (middlewareExchange *MessageMiddlewareExchangeRabbitMQ) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	queue, err := declareQueue(middlewareExchange.channel, "", false, true)

	if err != nil {
		return handleError(err, middlewareExchange.connection)
	}
	for _, key := range middlewareExchange.keys {
		err = middlewareExchange.channel.QueueBind(
			queue.Name,
			key,
			middlewareExchange.exchange,
			false,
			nil,
		)
		if err != nil {
			return handleError(err, middlewareExchange.connection)
		}
	}
	middlewareExchange.consuming = true
	deliveries, err := getConsumeChannel(middlewareExchange.channel, queue.Name, middlewareExchange.exchange)

	if err != nil {
		return handleError(err, middlewareExchange.connection)
	}
	consumeDeliveries(deliveries, callbackFunc)

	if middlewareExchange.consuming {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (middlewareExchange *MessageMiddlewareExchangeRabbitMQ) StopConsuming() error {
	if !middlewareExchange.consuming {
		return nil
	}
	middlewareExchange.consuming = false
	err := stopConsuming(middlewareExchange.channel, middlewareExchange.exchange)

	if err != nil {
		return handleError(err, middlewareExchange.connection)
	}
	return nil
}

func (middlewareExchange *MessageMiddlewareExchangeRabbitMQ) Send(msg Message) error {
	publishing := createPublishing(msg)

	for _, key := range middlewareExchange.keys {
		err := publish(middlewareExchange.channel, middlewareExchange.exchange, key, publishing)

		if err != nil {
			return handleError(err, middlewareExchange.connection)
		}
	}
	return nil
}

func (middlewareExchange *MessageMiddlewareExchangeRabbitMQ) Close() error {
	return closeConnection(middlewareExchange.connection)
}
