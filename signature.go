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

func (sig *Signature) clone() *Signature {
	newSig := new(Signature)
	newSig.Task = sig.Task
	newSig.Args = sig.Args
	return newSig
}
