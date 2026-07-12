package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/kenchi-huang/go-task-queue/models"
)

type EnqueueRequest struct {
	Count int `json:"count"`
}

type UpdateWorkerCountRequest struct {
	MaxWorkerCount int64 `json:"max_workers"`
	MinWorkerCount int64 `json:"min_workers"`
}

func main() {
	dispatcher := models.NewDispatcher(10, 1)
	dispatcher.Run()
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		data := make(map[string]any)
		data["Successes"] = dispatcher.SuccessCount.Load()
		data["Failures"] = dispatcher.FailedCount.Load()
		data["Active Workers"] = len(dispatcher.WorkerCancels)
		data["Queue Depth"] = len(dispatcher.JobQueue)

		jobMap := make(map[string]string)

		dispatcher.JobTracker.Range(func(key, value any) bool {
			// We have to cast the generic 'any' types back to their actual types
			jobID := key.(uuid.UUID).String()
			status := value.(string)

			jobMap[jobID] = status
			return true // Returning true tells the loop to keep going!
		})

		data["Jobs"] = jobMap

		err := json.NewEncoder(w).Encode(data)
		if err != nil {
			fmt.Println("Error occurred: " + err.Error())
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	http.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
		var req EnqueueRequest
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				return
			}
		}(r.Body)

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			return
		}

		for range req.Count {
			createDummyJob(dispatcher)
		}
	})

	http.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		var req UpdateWorkerCountRequest
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				return
			}
		}(r.Body)

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			return
		}

		dispatcher.UpdateWorkerCount(req.MaxWorkerCount, req.MinWorkerCount)
	})

	go func() {
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			return
		}
	}()

	<-sigChannel
	dispatcher.Stop()
}

func createDummyJob(dispatcher *models.Dispatcher) {
	newDummyJob := models.Job{
		ID:       uuid.New(),
		Priority: 0,
		DoJob: func() error {
			fmt.Println("Doing Job!")
			time.Sleep(2 * time.Second)
			return nil
		},
	}
	dispatcher.JobQueue <- newDummyJob
}
