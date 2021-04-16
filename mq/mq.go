package mq

type Msg struct {
	Name    string
	Payload interface{}
}

type msgHandler func(msg *Msg)
type msgHandlerInfo struct {
	Handler msgHandler
	IsOnce  bool
}

type MQ interface {
	Push(name string, payload interface{}) error
	Pop() *Msg
	Len() int
}
