# Extra features for streadway/amqp package. 
<a href="https://travis-ci.org/makasim/amqpextra"><img src="https://travis-ci.org/makasim/amqpextra.png?branch=master" alt="Build Status"></a>

## Dialer.

Provides:
* Auto reconnect.
* Context aware.
* Configured by WithXXX options.
* Dial multiple servers. 
* Notifies ready\unready\closed states.

## Consumer.

Provides:
* Auto reconnect.
* Context aware.
* Configured by WithXXX options.
* Can process messages in parallel.
* Adds message context.
* Detects queue deletion and reconnect.
* Notifies ready\unready\closed states. 

#### Consumer middlewares

The consumer could chain middlewares for a preprocessing received message.

Here's some built-in middlewares:
* [HasCorrelationID](consumer/middleware/has_correlation_id.go) - Nack message if has no correlation id
* [HasReplyTo](consumer/middleware/has_reply_to.go) - Nack message if has no reply to.
* [Logger](consumer/middleware/logger.go) - Context with logger.
* [Recover](consumer/middleware/recover.go) - Recover worker from panic, nack message.
* [Expire](consumer/middleware/expire.go) - Convert Message expiration to context with timeout.
* [AckNack](consumer/middleware/ack_nack.go) - Return middleware.Ack to ack message.

## Publisher.

Provides:
* Auto reconnect.
* Context aware.
* Configured by WithXXX options.
* Notifies ready\unready\closed states.
* Publish could wait till connection ready.
* Adds message context.
* Publish a message struct (define only what you need). 
* Supports [flow control](https://www.rabbitmq.com/flow-control.html). 
