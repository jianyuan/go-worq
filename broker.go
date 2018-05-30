package worq

type Broker interface {
	Consume(ctx Context, queueName string) (Consumer, error)
	Close() error
}
