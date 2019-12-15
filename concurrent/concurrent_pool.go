package concurrent

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrExexcutorCapacity = errors.New("协程池容量不合法")
var ErrExexcutorClosed = errors.New("协程池已关闭")

type Task interface {
	Handle()
}

type Executor struct {
	sync.Mutex
	capacity int
	active   int32
	workers  []*worker
	closed   bool
}

func NewExecutor(capacity int) (e *Executor, err error) {
	if capacity <= 0 {
		return nil, ErrExexcutorCapacity
	}
	return &Executor{
		capacity: capacity,
	}, nil
}

func (e *Executor) getWorker() *worker {
	var w *worker
	var wait bool
	e.Lock()
	n := len(e.workers) - 1
	//have active
	if n >= 0 {
		w = e.workers[n]
		e.workers[n] = nil
		e.workers = e.workers[:n]
	} else {
		wait = e.activing() >= e.capacity
	}
	e.Unlock()
	//wait
	if wait {
		for {
			e.Lock()
			n := len(e.workers) - 1
			if n < 0 {
				e.Unlock()
				continue
			}
			w = e.workers[n]
			e.workers[n] = nil
			e.workers = e.workers[:n]
			e.Unlock()
			return w
		}
	}
	if w == nil {
		//create
		w = &worker{
			e:        e,
			taskChan: make(chan func()),
		}
		w.run()
		e.incActive()
	}
	return w
}
func (e *Executor) recoverWorker(w *worker) {
	e.Lock()
	if !e.closed {
		e.workers = append(e.workers, w)
	} else {
		w.release()
	}
	e.Unlock()
}

func (e *Executor) Submit(task func()) error {
	if e.closed {
		return ErrExexcutorClosed
	}
	w := e.getWorker()
	fmt.Println("<-")
	w.taskChan <- task
	fmt.Println("<- end")
	return nil
}

func (e *Executor) SubmitSyncBatch(tasks []func()) error {
	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for _, t := range tasks {
		err := func(cb func()) error {
			err := e.Submit(func() {
				fmt.Println("done 0")
				cb()
				fmt.Println("done 1")
				wg.Done()
				fmt.Println("done 2")
			})
			return err
		}(t)
		if err != nil {
			return err
		}
	}
	wg.Wait()
	return nil
}

func (e *Executor) Close() {
	e.Lock()
	e.closed = true
	for _, w := range e.workers {
		w.release()
	}
	e.Unlock()
}

func (e *Executor) activing() int {
	return int(atomic.LoadInt32(&e.active))
}

func (e *Executor) incActive() {
	atomic.AddInt32(&e.active, 1)
}

func (e *Executor) decActive() {
	atomic.AddInt32(&e.active, -1)
}

type worker struct {
	e        *Executor
	taskChan chan func()
}

func (w *worker) run() {
	go func() {
		for {
			select {
			case task := <-w.taskChan:
				fmt.Println("WORK",task)
				if task == nil {
					fmt.Println("111")
					w.e.decActive()
					return
				}
				fmt.Println("222")
				task()
				fmt.Println("RECOVER")
				w.e.recoverWorker(w)
			}
		}
	}()
}

func (w *worker) release() {
	go func() {
		w.taskChan <- nil
	}()
}
