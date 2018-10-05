package celery

import (
	"encoding/json"
	"errors"

	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
)

const (
	MIMEApplicationJSON = "application/json"
)

var (
	ErrIDMissing              = errors.New("celery: task id missing from header")
	ErrTaskMissing            = errors.New("celery: task missing from header")
	ErrUnsupportedContentType = errors.New("celery: unsupported media type")
	ErrBadJSONBody            = errors.New("celery: bad JSON body")
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

func (Protocol) ID(msg worq.Message) (string, error) {
	if id, ok := amqpTableStringOk(msg.Headers(), "id"); ok {
		return id, nil
	}
	return "", ErrIDMissing
}

func (Protocol) Task(msg worq.Message) (string, error) {
	if task, ok := amqpTableStringOk(msg.Headers(), "task"); ok {
		return task, nil
	}
	return "", ErrTaskMissing
}

// TaskBody is a 3-length array with the shape: [TaskArgs, TaskKWArgs, TaskEmbed]
type TaskBody [3]json.RawMessage

type TaskArgs []interface{}

type TaskKWArgs map[string]interface{}

type TaskEmbed struct {
	Callbacks []*TaskSignature `json:"callbacks"`
	Errbacks  []*TaskSignature `json:"errbacks"`
	Chain     []*TaskSignature `json:"chain"`
	Chord     *TaskSignature   `json:"chord"`
}

type TaskSignature struct {
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	KWArgs  map[string]interface{} `json:"kwargs"`
	Options map[string]interface{} `json:"options"`
}
