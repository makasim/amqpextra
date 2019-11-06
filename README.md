# Extra features for streadway/amqp package. 

## Auto reconnecting.

The package provides a auto reconnect feature. The idea of my approach is to add as little abstraction as possible. In your code instead of using `*amqp.Connection` you should use `<-chan *amqp.Connection`. The channel returns a healthy connection. Subscribe to `chan *amqp.Error` to get notified when a connection is not helthy any more and you should get a new one with  `<-chan *amqp.Connection`. The channel `<-chan *amqp.Connection` is closed when you explicitly closed it with `connextra.Close()` method, otherwise, it tries to reconnect in background.

See an example ["examples/conn_example.go](examples/conn_example.go). 

