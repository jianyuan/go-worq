package amqpbroker

import (
	"errors"
	"fmt"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
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
	ch   *amqp.Channel
}

func New(connectionFactory ConnectionFactory, options ...OptionFunc) (*Broker, error) {
	b := &Broker{
		exchange:     "go-worq",
		exchangeType: "direct",
		connFactory:  connectionFactory,
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

func (msg *Message) Delivery() *amqp.Delivery {
	return msg.delivery
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
