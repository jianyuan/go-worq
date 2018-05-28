package worq

type Consumer interface {
	Next() bool
	Err() error
	Message() (Message, error)
	Close() error

	Ack(message Message) error
	Nack(message Message, requeue bool) error
}
