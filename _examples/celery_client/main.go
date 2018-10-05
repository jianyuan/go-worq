package main

import (
	"github.com/kr/pretty"
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

	type addArgs struct {
		X int `json:"x"`
		Y int `json:"y"`
	}

	for {
		logger.Infoln("Enqueuing")
		result, err := app.Enqueue(worq.NewSignature("tasks.add", &addArgs{X: 2, Y: 10}))
		if err != nil {
			panic(err)
		}
		pretty.Println(result)
	}
}
