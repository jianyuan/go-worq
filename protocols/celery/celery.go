package celery

import (
	"errors"

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

func (c *Protocol) ID(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.Message:
		if id, ok := msg.Delivery().Headers["id"]; ok {
			if id, ok := id.(string); ok {
				return id, nil
			}
		}
	}
	return "", ErrIDNotFound
}

func (c *Protocol) Task(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.Message:
		if id, ok := msg.Delivery().Headers["task"]; ok {
			if id, ok := id.(string); ok {
				return id, nil
			}
		}
	}
	return "", ErrTaskNotFound
}
