package amqpbroker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	uuid "github.com/gofrs/uuid"
	worq "github.com/jianyuan/go-worq"
	"github.com/streadway/amqp"
)

// OptionFunc is a function that configures the AMQPBroker.
type OptionFunc func(*Broker) error
type ConnectionFactory func() (*amqp.Connection, error)

var _ worq.Broker = (*Broker)(nil)

type Broker struct {
	exchange     string
	exchangeType string

	connFactory ConnectionFactory

	conn *amqp.Connection

	// TODO: Extract these into a session struct
	ch      *amqp.Channel
	confirm chan amqp.Confirmation
}

func New(connectionFactory ConnectionFactory, options ...OptionFunc) (*Broker, error) {
	b := &Broker{
		exchange:     "go-worq",
		exchangeType: "direct",
		connFactory:  connectionFactory,
		confirm:      make(chan amqp.Confirmation, 1),
	}

	for _, option := range options {
		if err := option(b); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func SetExchange(exchange, exchangeType string) OptionFunc {
	return func(b *Broker) error {
		b.exchange = exchange
		b.exchangeType = exchangeType
		return nil
	}
}

func (b *Broker) getConn() (*amqp.Connection, error) {
	if b.conn == nil {
		var err error
		b.conn, err = b.connFactory()
		if err != nil {
			return nil, err
		}
	}
	return b.conn, nil
}

func (b *Broker) getChannel() (*amqp.Channel, error) {
	if b.ch == nil {
		var conn *amqp.Connection
		var err error
		conn, err = b.getConn()
		if err != nil {
			return nil, err
		}

		b.ch, err = conn.Channel()
		if err != nil {
			return nil, err
		}

		// Put this channel into confirm mode
		err = b.ch.Confirm(false)
		if err != nil {
			// publisher confirms not supported
			// TODO: logging
			close(b.confirm)
		} else {
			b.ch.NotifyPublish(b.confirm)
		}
	}
	return b.ch, nil
}

func (b *Broker) Consume(ctx worq.Context, queueName string) (worq.Consumer, error) {
	var err error

	ch, err := b.getChannel()
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(
		b.exchange,     // name
		b.exchangeType, // kind
		true,           // durable
		false,          // autoDelete
		false,          // internal
		false,          // noWait
		nil,            // args
	); err != nil {
		return nil, err
	}

	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable,
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		return nil, err
	}

	if err := ch.QueueBind(
		queue.Name, // name
		queue.Name, // key
		b.exchange, // exchange
		false,      // noWait
		nil,        // args
	); err != nil {
		return nil, err
	}

	// TODO: customisation
	ctag := fmt.Sprintf("worq-%s", uuid.Must(uuid.NewV4()))

	// TODO: prefetch using ch.Qos()

	deliveries, err := ch.Consume(
		queue.Name, // queue
		ctag,       // tag
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		app:        ctx.App(),
		deliveries: deliveries,
		cancel: func() error {
			if ch != nil {
				return ch.Cancel(
					ctag, // consumer
					true, // noWait
				)
			}
			return nil
		},
	}

	// TODO: keep track of consumers

	return consumer, nil
}

func (b *Broker) Close() error {
	// TODO: cancel all active consumers
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

func (b *Broker) Enqueue(pub *worq.Publishing) error {
	if pub == nil {
		return errors.New("amqpbroker: Enqueue(nil)")
	}

	var err error

	ch, err := b.getChannel()
	if err != nil {
		return err
	}

	// TODO: Retries
	err = ch.Publish(
		b.exchange, // exchange
		pub.Queue,  // key
		false,      // mandatory
		false,      // immediate,
		amqp.Publishing{
			Headers:      pub.Headers,
			ContentType:  pub.ContentType,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         pub.Body,
		},
	)
	if err != nil {
		return err
	}

	if confirmed, ok := <-b.confirm; ok && !confirmed.Ack {
		return errors.New("amqpbroker.Enqueue: Failed to receive acknowledgement from broker")
	}

	// TODO: return publishing
	return nil
}

var _ worq.Consumer = (*Consumer)(nil)

type Consumer struct {
	app *worq.App

	deliveries <-chan amqp.Delivery
	cancel     func() error

	closemu sync.RWMutex
	closed  bool

	message *Message
	lasterr error
}

func (c *Consumer) Next() bool {
	doClose, ok := c.next()
	if doClose {
		c.Close()
	}
	return ok
}

func (c *Consumer) next() (doClose, ok bool) {
	c.closemu.RLock()
	defer c.closemu.RUnlock()

	if c.closed {
		return false, false
	}

	delivery, isOpen := <-c.deliveries
	if !isOpen {
		return true, false
	}

	c.message = &Message{
		app:      c.app,
		delivery: &delivery,
	}
	return false, true
}

func (c *Consumer) Err() error {
	return c.lasterr
}

func (c *Consumer) Message() (worq.Message, error) {
	c.closemu.RLock()
	defer c.closemu.RUnlock()

	if c.closed {
		return nil, errors.New("amqp: consumer is closed")
	}

	if c.message == nil {
		return nil, errors.New("amqp: Message called without calling Next")
	}

	return c.message, nil
}

func (c *Consumer) Close() error {
	return c.close(nil)
}

func (c *Consumer) close(err error) error {
	c.closemu.Lock()
	defer c.closemu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.lasterr == nil {
		c.lasterr = err
	}

	if c.cancel != nil {
		return c.cancel()
	}

	return nil
}

func (c *Consumer) Ack(msg worq.Message) error {
	return msg.(*Message).delivery.Ack(
		false, // multiple
	)
}

func (c *Consumer) Nack(msg worq.Message, requeue bool) error {
	return msg.(*Message).delivery.Nack(
		false,   // multiple
		requeue, // requeue
	)
}

var _ worq.Message = (*Message)(nil)

type Message struct {
	app      *worq.App
	delivery *amqp.Delivery
}

func (msg *Message) Queue() string {
	return msg.delivery.RoutingKey
}

func (msg *Message) ID() string {
	id, err := msg.app.Protocol().ID(msg)
	_ = err // TODO
	return id
}

func (msg *Message) Task() string {
	task, err := msg.app.Protocol().Task(msg)
	_ = err // TODO
	return task
}

func (msg *Message) Headers() map[string]interface{} {
	m := make(map[string]interface{}, len(msg.delivery.Headers))
	for k, v := range msg.delivery.Headers {
		m[k] = v
	}
	return m
}

func (msg *Message) ContentType() string {
	return msg.delivery.ContentType
}

func (msg *Message) Body() []byte {
	return msg.delivery.Body
}
