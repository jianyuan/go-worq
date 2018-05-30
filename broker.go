package worq

type Broker interface {
	Init(app *App) error
	Consume(queueName string) (Consumer, error)
	Close() error
}
