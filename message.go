package worq

type Message interface {
	Queue() string

	ID() string

	Task() string

	Headers() map[string]interface{}

	ContentType() string

	Body() []byte
}
