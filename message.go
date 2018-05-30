package worq

type Message interface {
	ID() string
	Task() string
}
