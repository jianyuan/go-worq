package worq

type Signature struct {
	App  *App
	Task string
	Args interface{}
	// TODO: callbacks, errbacks, chain, chord
}

func (sig *Signature) Queue() string {
	// TODO: task router
	return sig.App.defaultQueue
}
