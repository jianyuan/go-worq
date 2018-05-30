package main

import (
	"log"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
	"github.com/jianyuan/go-worq/brokers/amqpbroker"
	"github.com/jianyuan/go-worq/protocols/celery"
)

func main() {
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}
	logger.SetLevel(logrus.DebugLevel)

	broker, _ := amqpbroker.New(
		func() (*amqp.Connection, error) {
			return amqp.Dial("amqp://guest:guest@localhost:5672/")
		},
		amqpbroker.SetExchange("celery", "direct"),
	)

	app, _ := worq.New(
		worq.SetLogger(logger),
		worq.SetBroker(broker),
		worq.SetProtocol(celery.New()),
		worq.SetBinder(celery.NewBinder()),
		worq.SetDefaultQueue("celery"),
	)

	app.Register("tasks.add", func(ctx worq.Context) error {
		ctx.Logger().Info("tasks.add called!")

		var args struct {
			X int `json:"x"`
			Y int `json:"y"`
		}

		if err := ctx.Bind(&args); err != nil {
			return err
		}

		ctx.Logger().Infof("%d + %d = %d", args.X, args.Y, args.X+args.Y)

		return nil
	})

	log.Panic(app.Start())
}
