package mq

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewBaseMessageQueue(t *testing.T) {
	bmq := NewBaseMessageQueue(1)
	go func() {
		for {
			if bmq.Len() < 10 {
				<-time.After(time.Millisecond * 10)
			} else {
				<-time.After(time.Millisecond * 500)
			}
			bmq.Push("test", "test")
		}
	}()
	for {
		<-time.After(time.Millisecond * 100)
		msg := bmq.Pop()
		fmt.Println(time.Now(), msg.Name, bmq.Len())
	}
}

func TestSyncList_Push(t *testing.T) {
	bmq := SyncList{
		List:  list.New(),
		Mutex: sync.Mutex{},
	}
	go func() {
		for {
			if bmq.Len() < 10 {
				<-time.After(time.Millisecond * 10)
			} else {
				<-time.After(time.Millisecond * 500)
			}
			bmq.Push("test", "test")
		}
	}()
	for {
		<-time.After(time.Millisecond * 100)
		msg := bmq.Pop()
		fmt.Println(time.Now(), msg.Name, bmq.Len())
	}
}
