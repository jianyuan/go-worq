package worq

import (
	"github.com/sirupsen/logrus"
)

type Context interface {
	App() *App

	Logger() logrus.FieldLogger
}

var _ Context = (*context)(nil)

type context struct {
	app *App
}

func (ctx *context) App() *App {
	return ctx.app
}

func (ctx *context) Logger() logrus.FieldLogger {
	return ctx.app.logger
}
