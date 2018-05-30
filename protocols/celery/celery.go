package celery

import (
	"errors"

	worq "github.com/jianyuan/go-worq"
	"github.com/jianyuan/go-worq/brokers/amqpbroker"
)

var _ worq.Protocol = (*CeleryProtocol)(nil)

type CeleryProtocol struct {
}

func New() *CeleryProtocol {
	return &CeleryProtocol{}
}

func (c *CeleryProtocol) ID(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.AMQPMessage:
		if id, ok := msg.Delivery().Headers["id"]; ok {
			if id, ok := id.(string); ok {
				return id, nil
			}
		}
	}
	return "", errors.New("CeleryProtocol: ID not found")
}

func (c *CeleryProtocol) Task(msg worq.Message) (string, error) {
	switch msg := msg.(type) {
	case *amqpbroker.AMQPMessage:
		if id, ok := msg.Delivery().Headers["task"]; ok {
			if id, ok := id.(string); ok {
				return id, nil
			}
		}
	}
	return "", errors.New("CeleryProtocol: Task not found")
}
