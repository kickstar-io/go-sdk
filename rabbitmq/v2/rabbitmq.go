package v2

import (
	"fmt"
	_ "reflect"
	"sync"
	"time"

	"github.com/ponlv/go-kit/plog"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

var logger = plog.NewBizLogger("rabbitmq")

type Config struct {
	Uri                  string
	ChannelNotifyTimeout time.Duration
	Reconnect            struct {
		Interval   time.Duration
		MaxAttempt int
	}
	IndexName    string
	ExchangeName string
	ExchangeType string
	RoutingKey   string
	QueueName    string
	MainQueue    string
}

type RabbitMQ struct {
	mux                  sync.RWMutex
	config               Config
	dialConfig           amqp.Config
	connection           *amqp.Connection
	ChannelNotifyTimeout time.Duration
	ExchangeName         string
	ExchangeType         string
	RoutingKey           string
	QueueName            string
	MainQueue            string
	UniqueChannel        *amqp.Channel
	ChannelTrackArray    map[string]*amqp.Channel
	CurrRetryTime        int64
	OldRetryTime         int64
	Durable              bool
}

var RabbitConnector *RabbitMQ

// var ChannelTrackArray map[string]*amqp.Channel

func New(config Config) *RabbitMQ {
	return &RabbitMQ{
		config:               config,
		dialConfig:           amqp.Config{},
		ChannelNotifyTimeout: config.ChannelNotifyTimeout,
		UniqueChannel:        nil,
		ChannelTrackArray:    make(map[string]*amqp.Channel),
		OldRetryTime:         1,
		CurrRetryTime:        2,
	}
}

// Connect creates a new connection. Use once at application
// startup.
func (r *RabbitMQ) Connect() error {
	con, err := amqp.DialConfig(r.config.Uri, r.dialConfig)
	if err != nil {
		return err
	}
	r.connection = con
	go r.reconnect()
	return nil
}

// Channel returns a new `*amqp.Channel` instance. You must
// call `defer channel.Close()` as soon as you obtain one.
// Sometimes the connection might be closed unintentionally so
// as a graceful handling, try to connect only once.
func (r *RabbitMQ) Channel() (*amqp.Channel, error) {
	if r.connection == nil {
		if err := r.Connect(); err != nil {
			return nil, errors.New("connection is not open")
		}
	}
	// if r.UniqueChannel != nil {
	// 	log.Info("get exet channel to publish: ", *r.UniqueChannel)
	// 	return r.UniqueChannel, nil
	// }
	channel, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}
	// r.UniqueChannel = channel
	return channel, nil
}

// Channel returns a new `*amqp.Channel` instance. You must
// call `defer channel.Close()` as soon as you obtain one.
// Sometimes the connection might be closed unintentionally so
// as a graceful handling, try to connect only once.
func (r *RabbitMQ) ChannelByName(name string) (*amqp.Channel, error) {
	if r.connection == nil {
		if err := r.Connect(); err != nil {
			return nil, errors.New("connection is not open")
		}
	}
	storeChannel, errChannel := r.ChannelTrackArray[name]
	if !errChannel {
		channel, err := r.connection.Channel()
		if err != nil {
			return nil, err
		}
		r.ChannelTrackArray[name] = channel
		return channel, nil
	}
	return storeChannel, nil
}

// Connection exposes the essentials of the current connection.
// You should not normally use this but it is there for special
// use cases.
func (r *RabbitMQ) Connection() *amqp.Connection {
	return r.connection
}

// Shutdown triggers a normal shutdown. Use this when you wish
// to shutdown your current connection or if you are shutting
// down the application.
func (r *RabbitMQ) Shutdown() error {
	if r.connection != nil {
		return r.connection.Close()
	}
	r.cleanChannelTrack()
	r.UniqueChannel = nil
	return nil
}

// reconnect reconnects to server if the connection or a channel
// is closed unexpectedly. Normal shutdown is ignored. It tries
// maximum of 7200 times and sleeps half a second in between
// each try which equals to 1 hour.
func (r *RabbitMQ) reconnect() {
WATCH:

	conErr := <-r.connection.NotifyClose(make(chan *amqp.Error))
	if conErr != nil {
		logger.Error().Err(conErr).Msg("Connection dropped, reconnecting")
		r.cleanChannelTrack()
		r.UniqueChannel = nil
		var err error

		// for i := 1; i <= r.config.Reconnect.MaxAttempt; i++ {
		for {
			r.mux.RLock()
			r.connection, err = amqp.DialConfig(r.config.Uri, r.dialConfig)
			r.mux.RUnlock()

			if err == nil {
				goto WATCH
			}
			logger.Error().Err(err).Msg("Failed to reconnect")
			// time.Sleep(r.config.Reconnect.Interval)
			currRetryTime := r.CurrRetryTime
			newNextRetryTime := r.OldRetryTime + r.CurrRetryTime // next time retry in the future with fibo
			r.OldRetryTime = currRetryTime                       // in the future, current of now  become old
			r.CurrRetryTime = newNextRetryTime                   // for the future
			time.Sleep(time.Duration(currRetryTime) * time.Second)
		}
	} else {
		r.cleanChannelTrack()
		r.UniqueChannel = nil
	}
}

func (r *RabbitMQ) Ping() error {
	channel, err := r.Channel()
	if err != nil {
		r.UniqueChannel = nil
		return errors.Wrap(err, "failed to open channel")
	}
	r.UniqueChannel = channel
	defer channel.Close()
	return nil
}

func (r *RabbitMQ) Setup(name, exchange string, durable bool) *amqp.Queue {
	channel, err := r.Channel()
	if err != nil {
		return nil
	}
	defer channel.Close()
	q := r.DeclareQueue(channel, name, exchange, durable)
	return &q
}

func (r *RabbitMQ) DeclareQueue(ch *amqp.Channel, name string, exchange string, durable bool) amqp.Queue {
	ch, _ = r.Channel()
	defer ch.Close()
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

func (r *RabbitMQ) CreateTempQueue(callerId string, duration, autodelete, exclusive, nowait bool) error {
	channel, channelErr := r.ChannelByName(callerId)
	if channelErr != nil {
		logger.Error().Err(channelErr).Msg("failed to get channel to create queue")
		return errors.Wrap(channelErr, "failed to get channel to create queue")
	}
	if _, err := channel.QueueDeclare(
		r.config.QueueName+callerId,
		duration,
		autodelete,
		exclusive,
		nowait,
		amqp.Table{"x-queue-mode": "lazy"},
	); err != nil {
		return errors.Wrap(err, "failed to declare queue")
	}
	return nil
}
func (r *RabbitMQ) Consume(ch *amqp.Channel, q amqp.Queue, opt Option) <-chan amqp.Delivery {
	ch, _ = r.Channel()
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

func (r *RabbitMQ) SubcribleTopic(topicName, consumer string, autoAck, exclusive, noLocal, noWait bool) (<-chan amqp.Delivery, error) {
	channel, channelErr := r.ChannelByName(topicName)
	if channelErr != nil {
		logger.Error().Err(channelErr).Msg("failed to get channel to subscribe topic")
		return nil, errors.Wrap(channelErr, "failed to get channel to subscribe topic")
	}
	msgs, err := channel.Consume(r.config.QueueName+topicName, consumer, autoAck, exclusive, noLocal, noWait, amqp.Table{"x-queue-mode": "lazy"})
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}
	return msgs, nil
}

func (r *RabbitMQ) LeftTopic(topicName, consumer string, noWait bool) error {
	channel, channelErr := r.ChannelByName(topicName)
	if channelErr != nil {
		logger.Error().Err(channelErr).Msg("failed to get channel to left topic: ")
		return errors.Wrap(channelErr, "failed to get channel to left topic")
	}
	err := channel.Cancel(consumer, noWait)
	defer channel.Close()
	delete(r.ChannelTrackArray, topicName)
	if err != nil {
		return errors.Wrap(err, "failed to declare queue")
	}
	return nil
}
func (r *RabbitMQ) Publish(ch *amqp.Channel, q amqp.Queue, data []byte) error {
	channel, err := r.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer channel.Close()
	if err := channel.Confirm(false); err != nil {
		r.UniqueChannel = nil
		return errors.Wrap(err, "failed to put channel in confirmation mode")
	}
	if err := channel.Publish(
		"",
		q.Name,
		true,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	); err != nil {
		return errors.Wrap(err, "failed to publish message")
	}
	select {
	case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !ntf.Ack {
			return errors.New("failed to deliver message to exchange/queue")
		}
	case <-channel.NotifyReturn(make(chan amqp.Return)):
		return errors.New("failed to deliver message to exchange/queue")
	case <-time.After(r.ChannelNotifyTimeout):
		logger.Error().Err(err).Msg("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}

func (r *RabbitMQ) PublishContext(queueName string, data []byte) error {
	channel, err := r.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer channel.Close()
	if err := channel.Confirm(false); err != nil {
		r.UniqueChannel = nil
		return errors.Wrap(err, "failed to put channel in confirmation mode")
	}
	err = channel.Publish("", queueName, true, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        data,
	})
	if err != nil {
		return errors.Wrap(err, "failed to publish message")
	}
	select {
	case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !ntf.Ack {
			return errors.New("failed to deliver message to exchange/queue")
		}
	case <-channel.NotifyReturn(make(chan amqp.Return)):
		return errors.New("failed to deliver message to exchange/queue")
	case <-time.After(r.ChannelNotifyTimeout):
		logger.Error().Err(err).Msg("message delivery confirmation to exchange/queue timed out")
	}
	return nil
}

func (r *RabbitMQ) cleanChannelTrack() {
	for k := range r.ChannelTrackArray {
		delete(r.ChannelTrackArray, k)
	}
}
func (r *RabbitMQ) HandleMessages(qName string, isProduction bool, messages <-chan amqp.Delivery, f func(d []byte) error) {
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
