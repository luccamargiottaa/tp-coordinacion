package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareQueue(queueName, connection, channel)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	connection, channel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareExchange(exchange, keys, connection, channel)
}
