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
type OptionFunc func(*AMQPBroker) error
type ConnectionFactory func() (*amqp.Connection, error)

var _ worq.Broker = (*AMQPBroker)(nil)

type AMQPBroker struct {
	exchange     string
	exchangeType string

	connFactory ConnectionFactory

	app  *worq.App
	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(connectionFactory ConnectionFactory, options ...OptionFunc) (*AMQPBroker, error) {
	b := &AMQPBroker{
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
	return func(b *AMQPBroker) error {
		b.exchange = exchange
		b.exchangeType = exchangeType
		return nil
	}
}

func (b *AMQPBroker) Init(app *worq.App) error {
	b.app = app
	return nil
}

func (b *AMQPBroker) getConn() (*amqp.Connection, error) {
	if b.conn == nil {
		var err error
		b.conn, err = b.connFactory()
		if err != nil {
			return nil, err
		}
	}
	return b.conn, nil
}

func (b *AMQPBroker) getChannel() (*amqp.Channel, error) {
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

func (b *AMQPBroker) Consume(queueName string) (worq.Consumer, error) {
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

	consumer := &AMQPConsumer{
		app:        b.app,
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

func (b *AMQPBroker) Close() error {
	// TODO: cancel all active consumers
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

var _ worq.Consumer = (*AMQPConsumer)(nil)

type AMQPConsumer struct {
	app *worq.App

	deliveries <-chan amqp.Delivery
	cancel     func() error

	closemu sync.RWMutex
	closed  bool

	message *AMQPMessage
	lasterr error
}

func (c *AMQPConsumer) Next() bool {
	doClose, ok := c.next()
	if doClose {
		c.Close()
	}
	return ok
}

func (c *AMQPConsumer) next() (doClose, ok bool) {
	c.closemu.RLock()
	defer c.closemu.RUnlock()

	if c.closed {
		return false, false
	}

	delivery, isOpen := <-c.deliveries
	if !isOpen {
		return true, false
	}

	c.message = &AMQPMessage{
		app:      c.app,
		delivery: &delivery,
	}
	return false, true
}

func (c *AMQPConsumer) Err() error {
	return c.lasterr
}

func (c *AMQPConsumer) Message() (worq.Message, error) {
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

func (c *AMQPConsumer) Close() error {
	return c.close(nil)
}

func (c *AMQPConsumer) close(err error) error {
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

func (c *AMQPConsumer) Ack(msg worq.Message) error {
	return msg.(*AMQPMessage).delivery.Ack(
		false, // multiple
	)
}

func (c *AMQPConsumer) Nack(msg worq.Message, requeue bool) error {
	return msg.(*AMQPMessage).delivery.Nack(
		false,   // multiple
		requeue, // requeue
	)
}

var _ worq.Message = (*AMQPMessage)(nil)

type AMQPMessage struct {
	app      *worq.App
	delivery *amqp.Delivery
}

func (msg *AMQPMessage) Delivery() *amqp.Delivery {
	return msg.delivery
}

func (msg *AMQPMessage) ID() string {
	id, err := msg.app.Protocol().ID(msg)
	_ = err // TODO
	return id
}

func (msg *AMQPMessage) Task() string {
	task, err := msg.app.Protocol().Task(msg)
	_ = err // TODO
	return task
}
