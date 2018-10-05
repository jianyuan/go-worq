package worq

type Binder interface {
	Bind(ctx Context, v interface{}) error
	Unbind(ctx Context, id string, queue string, sig *Signature) (*Publishing, error)
}
