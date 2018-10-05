package celery

import (
	"fmt"
	"testing"

	worq "github.com/jianyuan/go-worq"
	"github.com/stretchr/testify/assert"
)

func TestBinder_implementsWorqBinder(t *testing.T) {
	assert.Implements(t, (*worq.Binder)(nil), new(Binder))
}

func TestBinder_Bind_errors(t *testing.T) {
	testCases := []struct {
		v         interface{}
		errString string
	}{
		{nil, "worq: Bind(nil)"},
		{struct{}{}, "worq: Bind(non-pointer struct {})"},
		{0, "worq: Bind(non-pointer int)"},
		{"", "worq: Bind(non-pointer string)"},
		{(*int)(nil), "worq: Bind(nil *int)"},
		{(*struct{})(nil), "worq: Bind(nil *struct {})"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("v=%#v", tc.v), func(t *testing.T) {
			b := new(Binder)
			ctx := new(worq.MockContext)
			err := b.Bind(ctx, tc.v)
			assert.EqualError(t, err, tc.errString)
		})
	}
}
