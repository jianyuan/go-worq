package celery

import (
	"encoding/json"
	"errors"
	"reflect"

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

var _ worq.Binder = (*Binder)(nil)

type Binder struct {
}

func NewBinder() *Binder {
	return &Binder{}
}

func (Binder) Bind(ctx worq.Context, v interface{}) error {
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)

	if rt == nil {
		return errors.New("worq: Bind(nil)")
	}

	if rv.IsNil() {
		return errors.New("worq: Bind(nil " + rt.String() + ")")
	}

	if rv.Kind() != reflect.Ptr {
		return errors.New("worq: Bind(non-pointer " + rt.String() + ")")
	}

	msg := ctx.Message()
	switch msg.ContentType() {
	case MIMEApplicationJSON:
		var body TaskBody
		if err := json.Unmarshal(msg.Body(), &body); err != nil {
			return err
		}

		var args []json.RawMessage
		if err := json.Unmarshal(body[0], &args); err != nil {
			return err
		}

		if len(args) > 0 {
			if rt.Elem().Kind() != reflect.Struct {
				return errors.New("worq: Bind(pointer to non-struct " + rt.String() + ")")
			}

			// TODO: Validation
			for i, arg := range args {
				f := rv.Elem().Field(i)
				if err := json.Unmarshal(arg, f.Addr().Interface()); err != nil {
					return err
				}
			}
		}

		// TODO: process position args
		ctx.Logger().Debug(string(msg.Body()))

		return json.Unmarshal(body[1], v)
	}
	return ErrUnsupportedContentType
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
