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

	"awesomequeue/handlers"

	"github.com/redis/go-redis/v9"
)

type server struct {
	rdb    *redis.Client
	queues []string
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
		listenAddr = "localhost:8080"
	}

	s := &server{
		rdb: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "",
			DB:       0,
		}),
	}

	http.HandleFunc("/reserve/", s.reserveHandler)
	http.HandleFunc("/delete/", s.deleteHandler)
	http.HandleFunc("/register/", s.registerWorkerHandler)
	http.HandleFunc("/unregister/", s.unregisterWorkerHandler)
	http.HandleFunc("/queue/", s.queueHandler)
	http.HandleFunc("/heartbeat/", s.heartbeatHandler)
	http.HandleFunc("/json-status/", s.jsonStatusHandler)
	http.HandleFunc("/status/", s.htmlStatusHandler)
	http.HandleFunc("/tubestatus/", s.tubeStatusHandler)
	http.HandleFunc("/job-status/", s.jobStatusHandler)

	slog.Info("Starting AwesomeQueue server, starting heartbeat goroutine")
	go handlers.CheckHeartbeatsLoop(s.rdb)
	slog.Info("Starting AwesomeQueue server", "listen", listenAddr, "redis", redisAddr)
	http.ListenAndServe(listenAddr, nil)
}

func (s *server) reserveHandler(w http.ResponseWriter, r *http.Request) {
	var worker handlers.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		slog.Error("error decoding worker json", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job, err := handlers.ReserveJob(r.Context(), s.rdb, worker.ID)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	var worker handlers.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		slog.Error("error decoding worker json", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := handlers.DeleteJob(r.Context(), s.rdb, worker.ID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, handlers.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
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
	workerID, err := handlers.RegisterWorker(r.Context(), s.rdb, queue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"workerID": workerID})
}

func (s *server) unregisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	var worker handlers.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		slog.Error("error decoding worker json", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := handlers.UnregisterWorker(r.Context(), s.rdb, worker.ID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"jobID": jobID})
}

func (s *server) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var worker handlers.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		slog.Error("error decoding worker json", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := handlers.UpdateHeartbeat(r.Context(), s.rdb, worker.ID)
	switch {
	case errors.Is(err, handlers.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) jsonStatusHandler(w http.ResponseWriter, r *http.Request) {
	status, err := handlers.GetJsonStatus(r.Context(), s.rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *server) htmlStatusHandler(w http.ResponseWriter, r *http.Request) {
	data, err := handlers.GetHtmlStatus(r.Context(), s.rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl := template.Must(template.ParseFiles(filepath.Join(os.Getenv("SNAP"), "templates", "status.html")))
	tmpl.Execute(w, data)
}

func (s *server) tubeStatusHandler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		http.Error(w, "invalid queue name: must not be empty", http.StatusBadRequest)
		return
	}
	status, err := handlers.GetTubeStatus(r.Context(), s.rdb, queue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// do to list queues
