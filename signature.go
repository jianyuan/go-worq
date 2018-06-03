package worq

type Signature struct {
	app  *App
	task string
	args interface{}
	// TODO: callbacks, errbacks, chain, chord
}

func NewSignature(task string, args interface{}) *Signature {
	return &Signature{
		task: task,
		args: args,
	}
}
