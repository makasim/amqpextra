# Extra features for streadway/amqp package. 

## Auto reconnecting.

The package provides an auto reconnect feature for [streadway/amqp](https://github.com/streadway/amqp). The approach idea is to add as little abstraction as possible. In your code instead of using `*amqp.Connection` you should use `<-chan *amqp.Connection`. The channel returns a healthy connection. You should subscribe to `chan *amqp.Error` to get notified when a connection is not helthy any more and you should request a new one via  `<-chan *amqp.Connection`. The channel `<-chan *amqp.Connection` is closed when you explicitly closed it by calling `connextra.Close()` method, otherwise, it tries to reconnect in background.

See an [example](examples/conn_example.go). 

## Consumer.

The package provides a handy consumer. It is aware of `<-chan *amqp.Connection` and `<-chan *amqp.Error` and can work with them respectively.
It also starts multiple works in background and correctly stop them when needed.  

See an [example](examples/consumer_example.go).

## Consumer Middleware

The consumer could chain middlewares for pre precessing received message. 
Check an example that rejects messages without correlation_id and reply_to properties.  

See an [example](examples/consumer_middleware.go).

## Publisher.

The package provides a handy publisher. It is aware of `<-chan *amqp.Connection` and `<-chan *amqp.Error` and can work with them respectively.  

See an [example](examples/publisher_example.go).