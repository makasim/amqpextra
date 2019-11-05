# Extra features for streadway/amqp package. 

## Auto reconnecting.

The package provides a connection wrapper that handles reconnection and notifies your code to get a fresh connection:

```go
package main

import (
    "log"

    "github.com/makasim/amqpextra"
    "github.com/streadway/amqp"
)

func main() {
    connextra := amqpextra.New(
        func() (*amqp.Connection, error) {
            return amqp.Dial("amqp://guest:guest@localhost:5672/%2f")
        },
        log.Printf,
        log.Printf,
    )

    for {
        conn, closeCh := connextra.Get()
        if conn == nil {
            log.Printf("connection permanently closed")
    
            return
        }
    
        ch, err := conn.Channel()
        if err != nil {
            log.Printf("amqp: create channel: %s", err)
    
            return
        }
        
        q, err := ch.QueueDeclare("test-queue", true, false, false, false, nil,)
        if err != nil {
            log.Printf("amqp: declare queue: %s", err)

            return 
        }
        
        msgCh, err := ch.Consume("test-queue", "", false, false, false, false, nil)
        if err != nil {
            log.Printf("amqp: consume: %s", err)
    
            return
        }

        log.Printf("amqp: consumption started")

        select {
        case msg := <-msgCh:
            // process message here
            log.Printf(string(msg.Body))
            msg.Ack(false)
        case err, ok := <-closeCh:
            if !ok {
                log.Printf("amqp: consumption stopped")               

                return 
            } 
            
            log.Printf("amqp: consumption stopped: %v", err)
            
            continue              
        }
    }
}
``` 


