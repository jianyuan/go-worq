package celery

import (
	"encoding/json"
	"errors"
	"reflect"

	worq "github.com/jianyuan/go-worq"
)

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

	if rv.Kind() != reflect.Ptr {
		return errors.New("worq: Bind(non-pointer " + rt.String() + ")")
	}

	if rv.IsNil() {
		return errors.New("worq: Bind(nil " + rt.String() + ")")
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
