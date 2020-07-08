package chief

import (
	"errors"
	"sync"
)

// ErrAlreadyStopped is returned from Chief.Start or Chief.Stop when chief is already stopped.
var ErrAlreadyStopped = errors.New("Master Chief is stopped")

// ErrAlreadyStarted is returned from Chief.Start when the method is called after master chief was already started.
var ErrAlreadyStarted = errors.New("Master Chief is already running")

// ErrNotRunning is returned from Chief.Stop when the method is called before master chief is started.
var ErrNotRunning = errors.New("Master Chief is not started yet")

type (
	// Task represents a task.
	Task interface{}

	// HandlerFunc processed a task.
	HandlerFunc func(Task)

	poolChannel chan chan Task

	worker struct {
		tasks   chan Task
		pool    poolChannel
		handler HandlerFunc
		stop    chan struct{}
		stopped chan<- struct{}
	}

	// Chief manages all the incoming tasks.
	Chief struct {
		NumWorkers int
		Handler    HandlerFunc

		tasks            chan Task
		pool             poolChannel
		lock             sync.Mutex
		stop             chan struct{}
		allWorkerStopped chan struct{}
		workerStopped    chan struct{}
		isStopped        bool
		isRunning        bool
	}
)

// Start starts master chief and makes him ready to receive tasks.
func (c *Chief) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isStopped {
		return ErrAlreadyStopped
	}

	if c.isRunning {
		return ErrAlreadyStarted
	}

	if c.Handler == nil {
		return errors.New("Handler is missing")
	}

	if c.NumWorkers < 1 {
		return errors.New("NumWorks must be greater then 0")
	}

	c.tasks = make(chan Task)
	c.stop = make(chan struct{})
	c.pool = make(poolChannel)
	c.allWorkerStopped = make(chan struct{})
	c.workerStopped = make(chan struct{})

	for i := 0; i < c.NumWorkers; i++ {
		worker{
			tasks:   make(chan Task),
			pool:    c.pool,
			handler: c.Handler,
			stop:    c.stop,
			stopped: c.workerStopped,
		}.Start()
	}

	go c.loop()

	c.isRunning = true

	return nil
}

// Stop stops master chief. The method blocks until all workers are done with the current task. No new task can be queued. All task in the which are waiting in the queue are ignored.
func (c *Chief) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isStopped {
		return ErrAlreadyStopped
	}

	if !c.isRunning {
		return ErrNotRunning
	}

	// send stop signal to all task which are waiting for a worker
	// and to chief it self to stop accepting new tasks.
	close(c.stop)
	<-c.allWorkerStopped

	c.isStopped = true

	return nil
}

// Queue queues a new task
func (c *Chief) Queue(task Task) {
	c.tasks <- task
}

func (c *Chief) loop() {
	for {
		select {
		case <-c.stop:
			for i := 0; i < c.NumWorkers; i++ {
				<-c.workerStopped
			}
			close(c.allWorkerStopped)
		case task := <-c.tasks:
			// Schedule a new task which is waiting for the next free worker.
			go func(t Task) {
				select {
				case <-c.stop:
					return
				case worker := <-c.pool:
					worker <- t
				}
			}(task)
		}
	}
}

func (w worker) Start() {
	go func() {
		for {

			select {
			case w.pool <- w.tasks:
			case task := <-w.tasks:
				w.handler(task)
			case <-w.stop:
				w.stopped <- struct{}{}
				return
			}
		}
	}()
}
