package worq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockContext_implementsContext(t *testing.T) {
	assert.Implements(t, (*Context)(nil), new(MockContext))
}
