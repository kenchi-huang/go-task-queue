package models

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type Worker struct {
	ID           uuid.UUID
	JobChannel   chan Job
	WorkerPool   chan chan Job
	Ctx          context.Context
	Wg           *sync.WaitGroup
	SuccessCount *atomic.Uint64
	FailedCount  *atomic.Uint64
	JobTracker   *sync.Map
}

func (w *Worker) Start() {
	go func() {
		defer w.Wg.Done()
		for {
			select {
			case w.WorkerPool <- w.JobChannel:
				select {
				case job := <-w.JobChannel:
					fmt.Println("Doing job: ", job.ID)
					w.JobTracker.Store(job.ID, "IN_PROGRESS")
					err := job.DoJob()
					if err != nil {
						fmt.Println("Error while executing job: ", job.ID, err)
						w.FailedCount.Add(1)
						w.JobTracker.Store(job.ID, "ERRORED")
						continue
					}
					w.JobTracker.Store(job.ID, "COMPLETED")
					w.SuccessCount.Add(1)
				case <-w.Ctx.Done():
					fmt.Println("Quitting worker.")
					return
				}
			case <-w.Ctx.Done():
				fmt.Println("Quitting worker.")
				return
			}

		}
	}()
}

func (w *Worker) Stop() {
	w.Ctx.Done()
}
