# Project Ponos: Go Task Queue Engine 🚀

A high-performance, purely in-memory, dynamically auto-scaling background task engine built in Go.

## 🏗️ Architecture & Concurrency Model

This engine was built from scratch without external frameworks to demonstrate mastery over Go's most advanced concurrency primitives.

### 1. The Worker Pool (Nested Channels)
At the core of the engine is a `chan chan Job`. Instead of a traditional mutex-locked slice, the Dispatcher acts as an event loop. Idle workers pass their personal, unbuffered inboxes (`chan Job`) into the Dispatcher's Master channel. The Dispatcher strictly pops a job off the queue, pulls an idle worker's inbox from the Master channel, and hands off the data. This guarantees zero lock contention and mathematically eliminates race conditions during job handoffs.

### 2. Dynamic Auto-Scaling (The Control Loop)
The engine does not rely on a static workforce. A background Goroutine acts as a "Control Loop" (similar to a Kubernetes HPA), waking up via `time.Ticker` every 250ms.
- **Scale Up:** If `len(JobQueue)` > 0, it dynamically spins up new background Goroutines to handle the load, up to a configurable `MaxWorkers` limit.
- **Scale Down:** When the queue empties, it utilizes `context.WithCancel` to precisely target and kill excess idle workers, freeing up RAM.

### 3. Graceful Shutdown (WaitGroups & os/signal)
Production systems must not drop inflight data when restarted. 
The application listens for OS-level interrupts (`syscall.SIGINT`, `syscall.SIGTERM`). Upon receiving a termination signal, the Dispatcher instantly stops accepting new HTTP requests, fires a broadcast `close(quit)` to all workers, and blocks the main thread using `sync.WaitGroup` until every inflight database transaction or active job has fully completed.

### 4. Lock-Free Telemetry (`sync/atomic`)
To power the live dashboard, the engine tracks successful and failed job throughput. To prevent data races among thousands of concurrent workers, it utilizes `sync/atomic.Uint64`. This leverages low-level hardware instructions to lock the CPU memory bus for the exact nanosecond of the increment, avoiding bulky software `sync.Mutex` locks.

### 5. Concurrent Map Tracking (`sync.Map`)
Every job's lifecycle (`IN_PROGRESS`, `COMPLETED`, `ERRORED`) is tracked in real-time. Because standard Go maps will panic under concurrent writes, the engine utilizes `sync.Map` to safely track thousands of state changes simultaneously.

## 💻 Tech Stack
- **Backend:** Go (Standard Library Only: `sync`, `atomic`, `context`, `net/http`)
- **Frontend Dashboard:** Vanilla HTML/CSS/JS (Glassmorphism UI)
- **Deployment Strategy:** Due to the purely in-memory nature of the `chan` architecture, this engine is designed for persistent, stateful container hosting (e.g., AWS EC2, Render, Railway) rather than Serverless functions (Vercel/Lambda).

## 🚀 Running Locally

1. Clone the repository.
2. Run the engine:
   ```bash
   go run main.go
   ```
4. Open `http://localhost:8080` in your browser.
5. Click **FLOOD QUEUE** to push 5,000 jobs into the engine and watch the workers dynamically scale up in real-time!

## 📹Gif
https://github.com/user-attachments/assets/dbfe81f6-b78c-40cf-8799-122cb0fcf8e1
