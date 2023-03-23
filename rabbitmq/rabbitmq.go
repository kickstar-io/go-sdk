package rabbitmq

import (
	"errors"
	"fmt"

	"github.com/ponlv/go-kit/plog"

	amqp "github.com/rabbitmq/amqp091-go"
)

var logger = plog.NewBizLogger("rabbitmq")

func (r *RabbitMQConfig) InitRabbitMq(config RabbitMQConfig) (*amqp.Connection, *amqp.Channel, error) {
	host := config.Host
	port := config.Port
	username := config.UserName
	password := config.Password
	vHost := config.Vhost
	rabbitmqConnString := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", username, password, host, port, vHost)
	conn, ch, err := r.connect(rabbitmqConnString, config.Exchange)
	return conn, ch, err
}

func (r *RabbitMQConfig) connect(url string, exchange string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		logger.Error().Err(err).Msg("failed to connect to RabbitMQ")
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		logger.Error().Err(err).Msg("failed to open channel rabbitmq")
		return nil, nil, err
	}
	_ = ch.ExchangeDeclare(
		exchange,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	return conn, ch, nil
}

func (r *RabbitMQConfig) DeclareQueue(ch *amqp.Channel, name string, exchange string, durable bool) amqp.Queue {
	q, err := ch.QueueDeclare(
		name,    // name
		durable, // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		logger.Error().Err(err).Var("name", name).Msg("failed to declare queue rabbitmq")
	}
	err = ch.QueueBind(
		q.Name,                                 // queue name
		fmt.Sprintf("%s.%s", exchange, q.Name), // routing key
		exchange,                               // exchange
		false,
		nil,
	)
	if err != nil {
		logger.Error().Err(err).Var("name", name).Msg("failed to binding queue rabbitmq")
	}
	logger.Info().Var("name", name).Msg("declare queue success")
	return q
}

func (r *RabbitMQConfig) Publish(ch *amqp.Channel, q amqp.Queue, data []byte) error {
	if ch == nil {
		return errors.New("channel is nil")
	}
	err := ch.Publish(
		r.Exchange, // exchange
		q.Name,     // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
	if err != nil {
		logger.Error().Err(err).Var("name", q.Name).Msg("failed to publish a message")
	}
	logger.Info().Var("name", q.Name).Msg("message published successfully")
	return err
}

func (r *RabbitMQConfig) Consume(ch *amqp.Channel, q amqp.Queue, opt Option) <-chan amqp.Delivery {
	messages, err := ch.Consume(
		q.Name,        // queue
		q.Name,        // consumer
		opt.AutoACK,   // auto-ack
		opt.Exclusive, // exclusive
		opt.NoLocal,   // no-local
		opt.NoWait,    // no-wait
		opt.Args,      // args
	)
	if err != nil {
		logger.Error().Err(err).Var("name", q.Name).Msg("failed to register consumer")
	}
	return messages
}

func (r *RabbitMQConfig) HandleMessages(qName string, isProduction bool, messages <-chan amqp.Delivery, f func(d []byte) error) {
	forever := make(chan bool)

	go func() {
		for d := range messages {
			logger.Info().Var("body", d.Body).Msg("receive a message")
			err := f(d.Body)

			if err != nil {
				logger.Error().Err(err).Var("name", qName).Var("body", d.Body).Msg("error when handle a message")
				errReject := d.Reject(isProduction)

				if errReject != nil {
					logger.Error().Err(errReject).Var("name", qName).Var("body", d.Body).Msg("error when requeue")
				}
			} else {
				//Không phải production thì sẽ autoAck
				if isProduction {
					errAck := d.Ack(false)

					if errAck != nil {
						logger.Error().Err(errAck).Var("name", qName).Var("body", d.Body).Msg("error when ack")
					}
				}

			}
		}
	}()

	<-forever
}

type Option struct {
	AutoACK   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

var DefaultOption = Option{
	AutoACK:   false,
	Exclusive: false,
	NoLocal:   false,
	NoWait:    false,
	Args:      nil,
}
