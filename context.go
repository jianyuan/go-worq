package worq

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Context interface {
	App() *App

	Logger() logrus.FieldLogger

	Consumer() Consumer

	Message() Message

	Bind(v interface{}) error

	Reject(requeue bool) error

	Deadline() (deadline time.Time, ok bool)

	Done() <-chan struct{}

	Err() error

	Value(key interface{}) interface{}
}

// An emptyCtx is never canceled, has no values, and has no deadline
// Taken from standard library
type emptyCtx int

func (*emptyCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyCtx) Done() <-chan struct{} {
	return nil
}

func (*emptyCtx) Err() error {
	return nil
}

func (*emptyCtx) Value(key interface{}) interface{} {
	return nil
}

type context struct {
	emptyCtx

	app      *App
	logger   logrus.FieldLogger
	consumer Consumer
	msg      Message
}

func (ctx *context) App() *App {
	return ctx.app
}

func (ctx *context) Logger() logrus.FieldLogger {
	if ctx.logger != nil {
		return ctx.logger
	}
	return ctx.app.logger
}

func (ctx *context) Consumer() Consumer {
	return ctx.consumer
}

func (ctx *context) Message() Message {
	return ctx.msg
}

func (ctx *context) Bind(v interface{}) error {
	return ctx.app.binder.Bind(ctx, v)
}

func (ctx *context) Reject(requeue bool) error {
	return &TaskRejected{Requeue: requeue}
}
