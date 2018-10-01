package worq

type Message interface {
	ID() string

	Task() string

	Headers() map[string]interface{}

	ContentType() string

	Body() []byte
}
