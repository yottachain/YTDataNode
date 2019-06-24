package util

import (
	"fmt"
	"sync"
)

type QueueNode struct {
	Pre *QueueNode
	Next *QueueNode

	data interface{}
}

type Queue struct {
	head *QueueNode
	sync.Mutex
	size uint
	p *QueueNode
}

func (q *Queue)Push(data interface{}){
	q.Lock()
	defer q.Unlock()
	node := new(QueueNode)
	node.data = data
	if q.head == nil {
		q.head = node
		q.p = node
	} else {
		q.p.Next = node
		q.p = node
	}
	q.size = q.size + 1
}

func (q *Queue)Pop() (interface{},error) {
	q.Lock()
	defer q.Unlock()
	if q.size == 0 {
		return nil, fmt.Errorf("queue length is 0")
	}
	data := q.head.data
	q.head = q.head.Next
	q.size = q.size - 1
	return data,nil
}

func (q *Queue)Len()uint{
	return q.size
}

func (q *Queue)Head() interface{} {
	return q.head.data
}

func (q *Queue)End() interface{} {
	return q.p.data
}

