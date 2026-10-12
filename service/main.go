package main

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"text/template"

	"pigeonmq/handlers"

	"github.com/redis/go-redis/v9"
)

type server struct {
	rdb    *redis.Client
	queues []string
}

// statusTemplateFuncs are helper functions exposed to templates/status.html.
var statusTemplateFuncs = template.FuncMap{
	// jobsWithStatus returns the subset of jobs whose Status is one of statuses,
	// letting the template split the job list into sections without mutating data.
	"jobsWithStatus": func(jobs []handlers.Job, statuses ...string) []handlers.Job {
		var filtered []handlers.Job
		for _, j := range jobs {
			if slices.Contains(statuses, j.Status) {
				filtered = append(filtered, j)
			}
		}
		return filtered
	},
}

const (
	PriorityUrgent float64 = 0
	PriorityHigh   float64 = 1
	PriorityMedium float64 = 2
	PriorityLow    float64 = 3
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = "localhost:11300"
	}

	s := &server{
		rdb: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "",
			DB:       0,
		}),
	}

	http.HandleFunc("/reserve/", s.reserveHandler)
	http.HandleFunc("/complete/", s.completeHandler)
	http.HandleFunc("/cancel/", s.cancelHandler)
	http.HandleFunc("/register/", s.registerWorkerHandler)
	http.HandleFunc("/unregister/", s.unregisterWorkerHandler)
	http.HandleFunc("/queue/", s.queueHandler)
	http.HandleFunc("/heartbeat/", s.heartbeatHandler)
	http.HandleFunc("/aggregate-status/", s.aggregateStatusHandler)
	http.HandleFunc("/status/", s.htmlStatusHandler)
	http.HandleFunc("/status.json/", s.overviewHandler)
	http.HandleFunc("/job-status/", s.jobStatusHandler)

	slog.Info("Starting PigeonMQ server, starting heartbeat goroutine")
	go handlers.CheckHeartbeatsLoop(s.rdb)
	slog.Info("Starting PigeonMQ server", "listen", listenAddr, "redis", redisAddr)
	http.ListenAndServe(listenAddr, nil)
}

func (s *server) reserveHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		http.Error(w, "invalid worker ID: must not be empty", http.StatusBadRequest)
		return
	}

	job, err := handlers.ReserveJob(r.Context(), s.rdb, workerID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, handlers.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, handlers.ErrNoJobAvailable):
		w.WriteHeader(http.StatusNoContent)
		return
	case err != nil:
		slog.Error("error reserving jobs", "err", err, "worker", workerID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)

	slog.Info("Reserve", "worker", workerID, "job", job.ID)
}

// completeHandler is called by a worker to mark its currently-assigned job
// done and free itself up for the next one.
func (s *server) completeHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		http.Error(w, "invalid worker ID: must not be empty", http.StatusBadRequest)
		return
	}

	err := handlers.CompleteJob(r.Context(), s.rdb, workerID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, handlers.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		slog.Error("error completing job", "err", err, "worker", workerID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// cancelHandler cancels a job that hasn't started building yet.
func (s *server) cancelHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job")
	if jobID == "" {
		http.Error(w, "invalid job: must not be empty", http.StatusBadRequest)
		return
	}

	err := handlers.CancelJob(r.Context(), s.rdb, jobID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, handlers.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		slog.Error("error cancelling job", "err", err, "job", jobID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) jobStatusHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job")
	if jobID == "" {
		http.Error(w, "invalid job: must not be empty", http.StatusBadRequest)
		return
	}
	job, err := handlers.GetJobStatus(r.Context(), s.rdb, jobID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		slog.Error("failed to get job status", "err", err, "job", job.ID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *server) registerWorkerHandler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		http.Error(w, "invalid queue name: must not be empty", http.StatusBadRequest)
		return
	}
	// a nickname is not requires and can be the empty string
	nickname := r.URL.Query().Get("nickname")
	workerID, err := handlers.RegisterWorker(r.Context(), s.rdb, queue, nickname)
	if err != nil {
		slog.Error("failed to register worker", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"workerID": workerID})

	slog.Info("New worker registered", "ID", workerID, "Nickname", nickname)
}

func (s *server) unregisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		http.Error(w, "invalid worker ID: must not be empty", http.StatusBadRequest)
		return
	}

	err := handlers.UnregisterWorker(r.Context(), s.rdb, workerID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		slog.Error("failed to unregister worker", "err", err, "worker", workerID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("Unregistering worker", "ID", workerID)
}

func (s *server) queueHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty body not allowed", http.StatusBadRequest)
		return
	}

	queue := r.URL.Query().Get("queue")
	if queue == "" {
		http.Error(w, "invalid queue name: must not be empty", http.StatusBadRequest)
		return
	}

	if !slices.Contains(s.queues, queue) {
		s.queues = append(s.queues, queue)
	}

	var priority float64
	priorityParam := r.URL.Query().Get("priority")
	if priorityParam == "" {
		priority = PriorityMedium
	} else {
		priority, err = strconv.ParseFloat(priorityParam, 64)
		if err != nil {
			http.Error(w, "invalid priority", http.StatusBadRequest)
			return
		}
		if priority != PriorityUrgent && priority != PriorityHigh && priority != PriorityMedium && priority != PriorityLow {
			http.Error(w, "invalid priority: must be urgent = 0, high = 1, medium = 2, or low = 3", http.StatusBadRequest)
			return
		}
	}

	jobID, err := handlers.QueueJob(r.Context(), s.rdb, body, priority, queue)
	if err != nil {
		slog.Error("error queueing job", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"jobID": jobID})
}

func (s *server) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	workerID := r.URL.Query().Get("worker")
	if workerID == "" {
		http.Error(w, "invalid worker ID: must not be empty", http.StatusBadRequest)
		return
	}

	err := handlers.UpdateHeartbeat(r.Context(), s.rdb, workerID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		slog.Error("failed to register heartbeat", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// aggregateStatusHandler returns worker/job counts as JSON. An optional ?queue=
// param restricts the counts to that tube; omitted, it covers all tubes.
func (s *server) aggregateStatusHandler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	status, err := handlers.GetAggregateStatus(r.Context(), s.rdb, queue)
	if err != nil {
		slog.Error("failed to get status summary", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// htmlStatusHandler renders the full overview (jobs, workers, dead workers) as
// an HTML page. An optional ?queue= param restricts it to that tube.
func (s *server) htmlStatusHandler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	data, err := handlers.GetStatusOverview(r.Context(), s.rdb, queue)
	if err != nil {
		slog.Error("failed to get status", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl := template.Must(template.New("status.html").Funcs(statusTemplateFuncs).ParseFiles(filepath.Join(os.Getenv("SNAP"), "templates", "status.html")))
	tmpl.Execute(w, data)
}

// overviewHandler returns the same full overview as htmlStatusHandler, but as
// JSON. An optional ?queue= param restricts it to that tube.
func (s *server) overviewHandler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	data, err := handlers.GetStatusOverview(r.Context(), s.rdb, queue)
	if err != nil {
		slog.Error("failed to get status", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// do to list queues
