package amqp

import (
	"errors"
	"fmt"
	"sync"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
)

type ConnectionCreator func() (*amqp.Connection, error)

var _ worq.Broker = (*AMQPBroker)(nil)

type AMQPBroker struct {
	Exchange     string
	ExchangeType string

	createConn ConnectionCreator

	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(connectionCreator ConnectionCreator) *AMQPBroker {
	return &AMQPBroker{
		Exchange:     "go-worq",
		ExchangeType: "direct",
		createConn:   connectionCreator,
	}
}

func (b *AMQPBroker) getConn() (*amqp.Connection, error) {
	if b.conn == nil {
		var err error
		b.conn, err = b.createConn()
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
		b.Exchange,     // name
		b.ExchangeType, // kind
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
		b.Exchange, // exchange
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
	delivery *amqp.Delivery
}
