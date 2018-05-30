package main

import (
	"log"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	worq "github.com/jianyuan/go-worq"
	"github.com/jianyuan/go-worq/brokers/amqpbroker"
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
		worq.SetDefaultQueue("celery"),
	)

	log.Panic(app.Start())
}
