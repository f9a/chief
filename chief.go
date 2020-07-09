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

	taskRequest struct {
		data interface{}
		done chan<- struct{}
	}

	// HandlerFunc processed a task.
	HandlerFunc func(Task)

	poolChannel chan chan taskRequest

	worker struct {
		tasks     chan taskRequest
		pool      poolChannel
		handler   HandlerFunc
		stop      chan struct{}
		stopped   chan<- struct{}
		dropTasks bool
	}

	// Chief manages all the incoming tasks.
	Chief struct {
		NumWorkers int
		Handler    HandlerFunc
		// If true Chief will not wait for tasks to be executed by a worker.
		// Only task which are already assigned to a worker will be finished and then the worker is stopped.
		OnStopDropTasks bool

		tasks            chan Task
		pool             poolChannel
		lock             sync.Mutex
		stop             chan struct{}
		stopTasks        chan struct{}
		stopWorkers      chan struct{}
		allWorkerStopped chan struct{}
		workerStopped    chan struct{}
		taskDone         chan struct{}
		isStopped        bool
		isRunning        bool
	}
)

func (c *Chief) stopping(openTasks int) {
	if c.OnStopDropTasks {
		close(c.stopTasks)
	} else {
		if openTasks > 0 {
			for {
				<-c.taskDone
				openTasks--
				if openTasks == 0 {
					break
				}
			}
		}
	}

	close(c.stopWorkers)
	for i := 0; i < c.NumWorkers; {
		select {
		case <-c.workerStopped:
			i++
		case <-c.taskDone:
		}
	}
	close(c.allWorkerStopped)
}

func (c *Chief) loop() {
	openTasks := 0
	for {
		select {
		case <-c.stop:
			c.stopping(openTasks)
			return
		case task := <-c.tasks:
			openTasks++
			// Schedule a new task which is waiting for the next free worker.
			go func(t Task) {
				select {
				case <-c.stopTasks:
					return
				case worker := <-c.pool:
					worker <- taskRequest{data: t, done: c.taskDone}
				}
			}(task)
		case <-c.taskDone:
			openTasks--
		}
	}
}

// Start starts master chief and makes him ready to receive tasks.
func (c *Chief) Start() error {
	c.lock.Lock()

	if c.isStopped {
		c.lock.Unlock()
		return ErrAlreadyStopped
	}

	if c.isRunning {
		c.lock.Unlock()
		return ErrAlreadyStarted
	}

	if c.Handler == nil {
		c.lock.Unlock()
		return errors.New("Handler is missing")
	}

	if c.NumWorkers < 1 {
		c.lock.Unlock()
		return errors.New("NumWorks must be greater then 0")
	}

	c.isRunning = true

	c.tasks = make(chan Task)
	c.stop = make(chan struct{})
	c.pool = make(poolChannel)
	c.allWorkerStopped = make(chan struct{})
	c.workerStopped = make(chan struct{})
	c.stopWorkers = make(chan struct{})
	c.stopTasks = make(chan struct{})
	c.taskDone = make(chan struct{})
	c.lock.Unlock()

	for i := 0; i < c.NumWorkers; i++ {
		worker{
			tasks:   make(chan taskRequest),
			pool:    c.pool,
			handler: c.Handler,
			stop:    c.stopWorkers,
			stopped: c.workerStopped,
		}.Start()
	}

	go c.loop()

	return nil
}

// Stop stops master chief. The method blocks until all workers are done with the current task. No new task can be queued.
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
	// and to chief itself to stop accepting new tasks.
	close(c.stop)
	<-c.allWorkerStopped

	c.isStopped = true

	return nil
}

// Queue queues a new task
func (c *Chief) Queue(task Task) {
	c.tasks <- task
}

func (w worker) Start() {
	go func() {
		for {

			select {
			case w.pool <- w.tasks:
			case task := <-w.tasks:
				w.handler(task.data)
				task.done <- struct{}{}
			case <-w.stop:
				w.stopped <- struct{}{}
				return
			}
		}
	}()
}
