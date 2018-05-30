package worq

type Protocol interface {
	ID(message Message) (string, error)
	Task(message Message) (string, error)
}
