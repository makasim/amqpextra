# Extra features for streadway/amqp package. 
<a href="https://travis-ci.org/makasim/amqpextra"><img src="https://travis-ci.org/makasim/amqpextra.png?branch=master" alt="Build Status"></a>

## Auto reconnecting.

The package provides an auto reconnect feature for [streadway/amqp](https://github.com/streadway/amqp). The approach idea is to add as little abstraction as possible. In your code instead of using `*amqp.Connection` you should use `<-chan *amqp.Connection`. The channel returns a healthy connection. You should subscribe to `chan *amqp.Error` to get notified when a connection is not helthy any more and you should request a new one via  `<-chan *amqp.Connection`. The channel `<-chan *amqp.Connection` is closed when you explicitly closed it by calling `connextra.Close()` method, otherwise, it tries to reconnect in background.

See an [example](examples/conn_example.go).

## Dial multiple hosts

The Dial method accepts a slice of connection URLs. It would round robin them till one works.

See an [example](examples/conn_example.go). 

## Consumer.

The package provides a handy consumer abstraction that works on top of `<-chan *amqp.Connection` and `<-chan *amqp.Error` channels.

See an [example](examples/consumer_example.go).

#### Workers

Consumer can start multipe works and spread the processing between them.

#### Context

Consumer supports context.Context. The context is passed to worker function. You can build timeout, cancelation strategies on top of it.

#### Middleware

The consumer could chain middlewares for pre precessing received message. 
Check an example that rejects messages without correlation_id and reply_to properties.  

See an [example](examples/consumer_middleware.go).

Some built-in middlewares:

* [HasCorrelationID](middleware/has_correlation_id.go) - Nack message if has no correlation id
* [HasReplyTo](middleware/has_reply_to.go) - Nack message if has no reply to.
* [Logger](middleware/logger.go) - Context with logger.
* [Recover](middleware/recover.go) - Recover worker from panic, nack message.
* [Expire](middleware/expire.go) - Convert Message expiration to context with timeout.
* [AckNack](middleware/ack_nack.go) - Return middleware.Ack to ack message.

## Publisher.

The package provides a handy publisher. It is aware of `<-chan *amqp.Connection` and `<-chan *amqp.Error` and can work with them respectively.  

See an [example](examples/publisher_example.go).
