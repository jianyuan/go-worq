package main

import (
	worq "github.com/jianyuan/go-worq"
	"github.com/sirupsen/logrus"
)

func main() {
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
	}

	app, _ := worq.New(
		worq.SetLogger(logger),
	)

	app.Start()
}
