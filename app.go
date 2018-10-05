package worq

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/sirupsen/logrus"
)

type TaskRejected struct {
	Requeue bool
}

func (t TaskRejected) Error() string {
	return fmt.Sprintf("worq: task rejected; requeue: %v", t.Requeue)
}

type TaskNotFound struct {
	Name string
}

func (t TaskNotFound) Error() string {
	return "worq: cannot find task " + t.Name
}

// OptionFunc is a function that configures the App.
type OptionFunc func(*App) error

type App struct {
	logger   logrus.FieldLogger
	broker   Broker
	protocol Protocol
	binder   Binder

	defaultQueue string
	idFunc       func() string

	taskMap sync.Map // map[string]TaskFunc
}

func New(options ...OptionFunc) (*App, error) {
	// Default logger
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}

	app := new(App)
	app.logger = logger
	app.defaultQueue = "worq"
	app.idFunc = func() string {
		return uuid.Must(uuid.NewV4()).String()
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

func (app *App) Register(name string, f TaskFunc) error {
	if name == "" {
		return errors.New("worq.Register: task name is empty")
	}

	if f == nil {
		return errors.New("worq.Register: task function is nil")
	}

	if _, dup := app.taskMap.LoadOrStore(name, f); dup {
		return errors.New("worq.Register: task already defined: " + name)
	}
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
	// TODO: Convert this into a consumer struct

	if msg, err := consumer.Message(); err == nil {
		// TODO: message specific context?
		ctx := &context{
			app: app,
			logger: app.logger.WithFields(logrus.Fields{
				"id":   msg.ID(),
				"task": msg.Task(),
			}),
			consumer: consumer,
			msg:      msg,
		}

		ctx.Logger().Info("Task received")

		switch err := app.processMessage(ctx).(type) {
		case nil:
			return consumer.Ack(msg)
		case *TaskNotFound:
			ctx.logger.Error(err)
			return consumer.Nack(msg, false)
		case *TaskRejected:
			ctx.logger.Warn(err)
			return consumer.Nack(msg, err.Requeue)
		default:
			ctx.logger.Error(err)
			return consumer.Nack(msg, true) // or requeue = false?
		}
	} else {
		return consumer.Nack(msg, false)
	}
}

func (app *App) processMessage(ctx Context) error {
	f, ok := app.taskMap.Load(ctx.Message().Task())
	if !ok {
		return &TaskNotFound{ctx.Message().Task()}
	}
	return f.(TaskFunc)(ctx)
}

func (app *App) Enqueue(sig *Signature) (*AsyncResult, error) {
	var err error

	queue := app.queueForSignature(sig)
	id := app.idFunc()

	publishing, err := app.binder.Unbind(app.Context(), id, queue, sig)
	if err != nil {
		return nil, err
	}

	err = app.broker.Enqueue(publishing)
	if err != nil {
		return nil, err
	}

	result := new(AsyncResult)
	result.ID = id
	return result, nil
}

func (app *App) queueForSignature(sig *Signature) string {
	// TODO: proper routing
	return app.defaultQueue
}

func (app *App) Binder() Binder {
	return app.binder
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
