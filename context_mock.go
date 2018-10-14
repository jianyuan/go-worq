package worq

import (
	"github.com/sirupsen/logrus"
)

type MockContext struct {
	emptyCtx

	MessageFactory func() Message
}

func NewMockContext() *MockContext {
	return &MockContext{}
}

func (ctx *MockContext) App() *App {
	return nil
}

func (ctx *MockContext) Logger() logrus.FieldLogger {
	return nil
}

func (ctx *MockContext) Consumer() Consumer {
	return nil
}

func (ctx *MockContext) Message() Message {
	return ctx.MessageFactory()
}

func (ctx *MockContext) Bind(v interface{}) error {
	return nil
}

func (ctx *MockContext) Reject(requeue bool) error {
	return nil
}
