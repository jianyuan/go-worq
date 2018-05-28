package worq

type Broker interface {
	Consume(queueName string) (Consumer, error)
	Close() error
}
