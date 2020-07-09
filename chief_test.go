package chief

import (
	"testing"
	"time"
)

func Test_StartSendJobStopChief(t *testing.T) {
	done := make(chan Task)
	fn := func(t Task) {
		done <- t
	}

	c := &Chief{
		NumWorkers: 1,
		Handler:    fn,
	}
	err := c.Start()
	if err != nil {
		t.Fatalf("Start shouldn't return an error: %v", err)
	}

	c.Queue(1)
	r := <-done
	if r != 1 {
		t.Fatalf("Want 1, got %v", r)
	}

	c.Queue(2)
	r = <-done
	if r != 2 {
		t.Fatalf("Want 2, got %v", r)
	}

	err = c.Stop()
	if err != nil {
		t.Fatalf("Stop shouldn't return an error: %v", err)
	}
}

func Test_StopAndDontWaitForAllTaskToFinish(t *testing.T) {
	done := make(chan Task, 1)
	fn := func(t Task) {
		time.Sleep(2 * time.Second)
		done <- t
	}

	c := &Chief{
		NumWorkers:      1,
		Handler:         fn,
		OnStopDropTasks: true,
	}
	err := c.Start()
	if err != nil {
		t.Fatalf("Start shouldn't return an error: %v", err)
	}

	c.Queue(1)
	<-done

	c.Queue(2)

	err = c.Stop()
	if err != nil {
		t.Fatalf("Stop shouldn't return an error: %v", err)
	}
}

func Test_StopAndWaitForAllTaskToFinish(t *testing.T) {
	done := make(chan Task, 1)
	fn := func(t Task) {
		time.Sleep(1 * time.Second)
		done <- t
	}

	c := &Chief{
		NumWorkers: 1,
		Handler:    fn,
	}
	err := c.Start()
	if err != nil {
		t.Fatalf("Start shouldn't return an error: %v", err)
	}

	c.Queue(1)
	<-done

	c.Queue(2)

	err = c.Stop()
	if err != nil {
		t.Fatalf("Stop shouldn't return an error: %v", err)
	}

	<-done
}

func Test_StartStopChief(t *testing.T) {
	fn := func(_ Task) {
	}

	c := &Chief{}
	err := c.Start()
	if err.Error() != "Handler is missing" {
		t.Fatalf("Want 'Handler is missing' error, got %v", err)
	}

	c = &Chief{
		Handler: fn,
	}
	err = c.Start()
	if err.Error() != "NumWorks must be greater then 0" {
		t.Fatalf("Want 'NumWorks must be greater then 0' error, got %v", err)
	}

	c = &Chief{
		NumWorkers: 1,
		Handler:    fn,
	}

	err = c.Stop()
	if err != ErrNotRunning {
		t.Fatalf("Want ErrNotRunning, got %v", err)
	}

	err = c.Start()
	if err != nil {
		t.Fatalf("Start shouldn't return an error: %v", err)
	}

	err = c.Start()
	if err != ErrAlreadyStarted {
		t.Fatalf("Want ErrAlreadyStarted, got %v", err)
	}

	err = c.Stop()
	if err != nil {
		t.Fatalf("Stop shouldn't return an error: %v", err)
	}

	err = c.Stop()
	if err != ErrAlreadyStopped {
		t.Fatalf("Want ErrAlreadyStopped, got %v", err)
	}

	err = c.Start()
	if err != ErrAlreadyStopped {
		t.Fatalf("Want ErrAlreadyStopped, got %v", err)
	}
}

func Test_StartManyWorkersSendJobsStopChief(t *testing.T) {
	done := make(chan struct{})
	fn := func(_ Task) {
		done <- struct{}{}
	}

	c := &Chief{
		NumWorkers: 10,
		Handler:    fn,
	}

	err := c.Start()
	if err != nil {
		t.Fatalf("Start shoudln't return an error: %v", err)
	}
	c.Queue(nil)
	c.Queue(nil)
	c.Queue(nil)
	c.Queue(nil)

	for i := 0; i < 4; i++ {
		<-done
	}

	err = c.Stop()
	if err != nil {
		t.Fatalf("Stop shouldn't return an error: %v", err)
	}
}
