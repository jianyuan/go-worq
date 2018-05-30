package celery

import (
	"errors"

	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
	"github.com/jianyuan/go-worq/brokers/amqpbroker"
)

var (
	ErrIDNotFound   = errors.New("celery: ID not found in header")
	ErrTaskNotFound = errors.New("celery: Task not found in header")
)

var _ worq.Protocol = (*Protocol)(nil)

type Protocol struct {
}

func New() *Protocol {
	return &Protocol{}
}

func amqpTableStringOk(t amqp.Table, key string) (string, bool) {
	if value, ok := t[key]; ok {
		if value, ok := value.(string); ok {
			return value, true
		}
	}
	return "", false
}

func (c *Protocol) ID(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.Message:
		if id, ok := amqpTableStringOk(msg.Delivery().Headers, "id"); ok {
			return id, nil
		}
	}
	return "", ErrIDNotFound
}

func (c *Protocol) Task(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.Message:
		if task, ok := amqpTableStringOk(msg.Delivery().Headers, "task"); ok {
			return task, nil
		}
	}
	return "", ErrTaskNotFound
}
