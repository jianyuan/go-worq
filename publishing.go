package worq

type Publishing struct {
	ID          string
	Queue       string
	Headers     map[string]interface{}
	ContentType string
	Body        []byte
}
