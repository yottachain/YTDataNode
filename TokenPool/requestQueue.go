package TokenPool

type request struct {
	Level int32
	Res   chan *Token
}

func NewRequest(level int32) *request {
	var req = new(request)
	req.Level = level
	req.Res = make(chan *Token)
	return req
}
