package worq

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
)

// OptionFunc is a function that configures the App.
type OptionFunc func(*App) error

type App struct {
	logger logrus.FieldLogger
	broker Broker

	defaultQueue string
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

func (app *App) Start() error {
	// TODO: implement me
	app.logger.Info("Watch this space")

	consumer, err := app.broker.Consume(app.defaultQueue)
	if err != nil {
		return err
	}

	for consumer.Next() {
		msg, _ := consumer.Message()
		app.logger.Debug(spew.Sdump(msg))
		consumer.Ack(msg)
	}

	return consumer.Err()
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

func SetDefaultQueue(queue string) OptionFunc {
	return func(app *App) error {
		app.defaultQueue = queue
		return nil
	}
}
