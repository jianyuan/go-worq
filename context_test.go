package worq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContext_implementsContext(t *testing.T) {
	assert.Implements(t, (*Context)(nil), new(context))
}
