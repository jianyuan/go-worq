package worq

import (
	"errors"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	ErrTaskFuncIsNil = errors.New("worq: task function is nil")
	ErrTaskNotFound  = errors.New("worq: task not found")
)

// OptionFunc is a function that configures the App.
type OptionFunc func(*App) error

type App struct {
	logger   logrus.FieldLogger
	broker   Broker
	protocol Protocol
	binder   Binder

	defaultQueue string

	tasks sync.Map
}

func New(options ...OptionFunc) (*App, error) {
	// Default logger
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}

	app := &App{
		logger:       logger,
		defaultQueue: "worq",
	}

	// Apply option functions
	for _, option := range options {
		if err := option(app); err != nil {
			return nil, err
		}
	}

	return app, nil
}

func (app *App) Context() Context {
	return &context{
		app: app,
	}
}

func (app *App) Protocol() Protocol {
	return app.protocol
}

func (app *App) Register(task string, f TaskFunc) error {
	if f == nil {
		return ErrTaskFuncIsNil
	}
	app.tasks.Store(task, f)
	return nil
}

func (app *App) Start() error {
	// TODO: implement me
	app.logger.Info("Watch this space")

	consumer, err := app.broker.Consume(app.Context(), app.defaultQueue)
	if err != nil {
		return err
	}

	for consumer.Next() {
		// TODO: Set up worker pool
		if err := app.consumerOnNext(consumer); err != nil {
			app.logger.Errorf("error consuming message: %v", err)
		}
	}

	return consumer.Err()
}

func (app *App) consumerOnNext(consumer Consumer) error {
	if msg, err := consumer.Message(); err == nil {
		switch err := app.processMessage(msg); err {
		case nil:
			return consumer.Ack(msg)
		case ErrTaskNotFound:
			app.logger.Error(err)
			return consumer.Nack(msg, false)
		default:
			app.logger.Error(err)
			return consumer.Nack(msg, true) // or requeue = false?
		}
	} else {
		return consumer.Nack(msg, false)
	}
}

func (app *App) processMessage(msg Message) error {
	app.logger.Infof("Task ID: %s", msg.ID())
	app.logger.Infof("Task: %s", msg.Task())

	f, ok := app.tasks.Load(msg.Task())
	if !ok {
		return ErrTaskNotFound
	}

	// TODO: message specific context?
	ctx := &context{
		app: app,
		logger: app.logger.WithFields(logrus.Fields{
			"id":   msg.ID(),
			"task": msg.Task(),
		}),
		msg: msg,
	}
	return f.(TaskFunc)(ctx)
}

// SetLogger sets the logger that the app will use.
func SetLogger(logger logrus.FieldLogger) OptionFunc {
	return func(app *App) error {
		app.logger = logger
		return nil
	}
}

func SetBroker(broker Broker) OptionFunc {
	return func(app *App) error {
		app.broker = broker
		return nil
	}
}

func SetProtocol(protocol Protocol) OptionFunc {
	return func(app *App) error {
		app.protocol = protocol
		return nil
	}
}

func SetBinder(binder Binder) OptionFunc {
	return func(app *App) error {
		app.binder = binder
		return nil
	}
}

func SetDefaultQueue(queue string) OptionFunc {
	return func(app *App) error {
		app.defaultQueue = queue
		return nil
	}
}
