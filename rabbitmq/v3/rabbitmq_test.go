package rabbitmq

import (
	"context"
	"fmt"
	"testing"
)

func TestPublish(t *testing.T) {
	Init("amqp://ahamove:PqlNFSSAZ35T@aff69993c60774ea9aaed2fec6ddee57-dc019ca240eefa8c.elb.ap-southeast-1.amazonaws.com:5672/")

	DeclareQueue(context.Background(), "12345", "12345")
	Publish(context.Background(), "12345", "125", "string")

}

func TestConsume(t *testing.T) {
	Init("amqp://ahamove:PqlNFSSAZ35T@aff69993c60774ea9aaed2fec6ddee57-dc019ca240eefa8c.elb.ap-southeast-1.amazonaws.com:5672/")

	//forever := make(chan bool)
	DeclareQueue(context.Background(), "12345", "12345")
	ConsumerParallel(context.Background(), "12345", "12345", 4, handle)

	//<-forever
}

func handle(b []byte) error {

	fmt.Println(b)

	return nil
}
