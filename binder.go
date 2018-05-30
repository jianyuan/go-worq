package worq

type Binder interface {
	Bind(ctx Context, v interface{}) error
}
