package recover

type Task struct {
	SnID        int32
	Data        []byte
	ExpriedTime int64
	TaskLife    int32
	SrcNodeID   int32
}
