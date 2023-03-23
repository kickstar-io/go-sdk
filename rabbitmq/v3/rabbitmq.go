package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ponlv/go-kit/plog"

	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/publisher"
	amqp "github.com/rabbitmq/amqp091-go"
)

var dialer *amqpextra.Dialer
var logger = plog.NewBizLogger("rabbitmq")

func Init(uri string) error {
	var err error

	dialer, err = amqpextra.NewDialer(amqpextra.WithURL(uri))
	if err != nil {
		return err
	}

	logger.Info().Msg("connection to RabbitMQ is established")
	return nil
}

func Publish(ctx context.Context, exchange, queue string, body interface{}) error {

	if dialer == nil {
		logger.Error().Err(errors.New("rabbitmq: no dialer available")).Send()
		return errors.New("rabbitmq: no dialer available")
	}

	p, err := amqpextra.NewPublisher(
		dialer.ConnectionCh(),
		publisher.WithRestartSleep(0),
	)
	if err != nil {
		fmt.Println(err)
	}
	defer p.Close()

	bytes, err := json.Marshal(body)
	if err != nil {
		fmt.Println(err)
	}

	err = p.Publish(publisher.Message{
		Context:   ctx,
		Exchange:  exchange,
		Key:       fmt.Sprintf("%s.%s", exchange, queue),
		Mandatory: true,
		Immediate: false,
		Publishing: amqp.Publishing{
			ContentType: "text/plain",
			Body:        bytes,
		},
	})

	if err != nil {
		logger.Error().Err(err).Var("key", fmt.Sprintf("%s.%s", exchange, queue)).Msg("publish failed")
		return err
	}

	logger.Info().Msg("publish succeeded")
	return nil
}

func DeclareQueue(ctx context.Context, exchange, queue string) error {

	if dialer == nil {
		logger.Error().Err(errors.New("rabbitmq: no dialer available")).Send()
		return errors.New("rabbitmq: no dialer available")
	}

	con, err := dialer.Connection(ctx)
	if err != nil {
		return err
	}
	ch, err := con.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	// create exchange
	err = ch.ExchangeDeclare(exchange,
		amqp.ExchangeTopic,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		amqp.Table{}) // arguments)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,
		fmt.Sprintf("%s.%s", exchange, q.Name),
		exchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func Consumer(ctx context.Context, exchange, queue string, handle func([]byte) error) {

	if dialer == nil {
		logger.Error().Err(errors.New("rabbitmq: no dialer available")).Send()
		return
	}
	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		bb := msg.Body
		err := handle(bb)
		if err != nil {
			fmt.Println(err)
		}
		return msg.Ack(false)
	})

	_, err := dialer.Consumer(
		consumer.WithContext(ctx),
		consumer.WithQueue(queue),
		consumer.WithHandler(h),
	)
	if err != nil {
		return
	}
	<-ctx.Done()
	return
}

func ConsumerParallel(ctx context.Context, exchange, queue string, numWorkers int, handle func([]byte) error) {

	if dialer == nil {
		logger.Error().Err(errors.New("rabbitmq: no dialer available")).Send()
		return
	}

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		bb := msg.Body
		err := handle(bb)
		if err != nil {
			fmt.Println(err)
		}
		return msg.Ack(false)
	})

	c, err := dialer.Consumer(
		consumer.WithContext(ctx),
		//consumer.WithExchange(exchange, fmt.Sprintf("%s.%s", exchange, queue)),
		consumer.WithQueue(queue),
		consumer.WithHandler(h),
		consumer.WithWorker(consumer.NewParallelWorker(numWorkers)),
	)
	if err != nil {
		return
	}
	defer c.Close()
	<-ctx.Done()
	return
}
