package worq

import (
	"github.com/sirupsen/logrus"
)

type Context interface {
	App() *App

	Logger() logrus.FieldLogger

	Message() Message

	Bind(v interface{}) error
}

var _ Context = (*context)(nil)

type context struct {
	app    *App
	logger logrus.FieldLogger
	msg    Message
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

func (ctx *context) Message() Message {
	return ctx.msg
}

func (ctx *context) Bind(v interface{}) error {
	return ctx.app.binder.Bind(ctx, v)
}
