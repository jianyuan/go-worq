package worq

type Consumer interface {
	Next() bool
	Err() error
	Message() (Message, error)
	Close() error
}
