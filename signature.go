package worq

type Signature struct {
	Task string
	Args interface{}
	// TODO: callbacks, errbacks, chain, chord
}

func NewSignature(task string, args interface{}) *Signature {
	return &Signature{
		Task: task,
		Args: args,
	}
}
