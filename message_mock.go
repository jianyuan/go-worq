package worq

type MockMessage struct {
	MockQueue       string
	MockID          string
	MockTask        string
	MockHeaders     map[string]interface{}
	MockContentType string
	MockBody        []byte
}

func (msg *MockMessage) Queue() string {
	return msg.MockQueue
}

func (msg *MockMessage) ID() string {
	return msg.MockID
}

func (msg *MockMessage) Task() string {
	return msg.MockTask
}

func (msg *MockMessage) Headers() map[string]interface{} {
	return msg.MockHeaders
}

func (msg *MockMessage) ContentType() string {
	return msg.MockContentType
}

func (msg *MockMessage) Body() []byte {
	return msg.MockBody
}
