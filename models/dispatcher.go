package models

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Dispatcher struct {
	Master        chan chan Job
	MaxWorkers    atomic.Int64
	MinWorkers    atomic.Int64
	JobQueue      chan Job
	Quit          chan bool
	WorkerCancels []context.CancelFunc
	ticker        *time.Ticker
	Wg            *sync.WaitGroup
	SuccessCount  atomic.Uint64
	FailedCount   atomic.Uint64
	JobTracker    sync.Map
}

func NewDispatcher(maxWorkers int64, minWorkers int64) *Dispatcher {
	d := &Dispatcher{
		Master:   make(chan chan Job),
		JobQueue: make(chan Job, 10000),
		Quit:     make(chan bool),
		ticker:   time.NewTicker(100 * time.Millisecond),
		Wg:       &sync.WaitGroup{},
	}

	d.MaxWorkers.Store(maxWorkers)
	d.MinWorkers.Store(minWorkers)

	return d
}

func (d *Dispatcher) UpdateWorkerCount(maxWorkers int64, minWorkers int64) {
	d.MaxWorkers.CompareAndSwap(d.MaxWorkers.Load(), maxWorkers)
	d.MinWorkers.CompareAndSwap(d.MinWorkers.Load(), minWorkers)
}

func (d *Dispatcher) startScaler() {
	go func() {
		for {
			select {
			case <-d.ticker.C:
				depthOfQueue := len(d.JobQueue)
				if depthOfQueue > 0 && len(d.WorkerCancels) < int(d.MaxWorkers.Load()) {
					d.hireWorker()
				} else if depthOfQueue == 0 && len(d.WorkerCancels) > int(d.MinWorkers.Load()) {
					d.fireWorker()
				}
			case <-d.Quit:
				return
			}
		}
	}()
}

func (d *Dispatcher) hireWorker() {
	fmt.Println("Creating new worker")
	ctx, cancel := context.WithCancel(context.Background())
	worker := Worker{
		ID:           uuid.New(),
		JobChannel:   make(chan Job),
		WorkerPool:   d.Master,
		Ctx:          ctx,
		Wg:           d.Wg,
		SuccessCount: &d.SuccessCount,
		FailedCount:  &d.FailedCount,
		JobTracker:   &d.JobTracker,
	}
	d.WorkerCancels = append(d.WorkerCancels, cancel)
	d.Wg.Add(1)
	worker.Start()
}

func (d *Dispatcher) fireWorker() {
	fmt.Println("Firing worker")
	d.WorkerCancels[len(d.WorkerCancels)-1]()
	d.WorkerCancels = d.WorkerCancels[:len(d.WorkerCancels)-1]
}

func (d *Dispatcher) Run() {
	for i := 0; i < int(d.MinWorkers.Load()); i++ {
		d.hireWorker()
	}
	go func() {
		d.startScaler()
		for {
			select {
			case job := <-d.JobQueue:
				idleWorker := <-d.Master
				idleWorker <- job
			case <-d.Quit:
				fmt.Println("Quitting dispatcher.")
				return
			}
		}
	}()
}

func (d *Dispatcher) Stop() {
	for _, cancel := range d.WorkerCancels {
		cancel()
	}
	close(d.Quit)
	d.Wg.Wait()
}
