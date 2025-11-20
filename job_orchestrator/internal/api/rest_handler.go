package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/tafolabi009/backend/job_orchestrator/internal/service"
)

type RESTHandler struct {
	orchestrator *service.OrchestratorService
}

func NewRESTHandler(orchestrator *service.OrchestratorService) *RESTHandler {
	return &RESTHandler{
		orchestrator: orchestrator,
	}
}

func (h *RESTHandler) SetupRoutes(router *mux.Router) {
	router.HandleFunc("/health", h.HealthCheck).Methods("GET")
	router.HandleFunc("/metrics", h.GetMetrics).Methods("GET")

	api := router.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/jobs", h.CreateJob).Methods("POST")
	api.HandleFunc("/jobs/{id}", h.GetJob).Methods("GET")
	api.HandleFunc("/jobs", h.ListJobs).Methods("GET")
	api.HandleFunc("/jobs/{id}/cancel", h.CancelJob).Methods("POST")
	api.HandleFunc("/pipelines/validation", h.CreateValidationPipeline).Methods("POST")
	api.HandleFunc("/pipelines/full", h.CreateFullPipeline).Methods("POST")
	api.HandleFunc("/pipelines/{id}", h.GetPipeline).Methods("GET")
	api.HandleFunc("/resources/status", h.GetResourceStatus).Methods("GET")
	api.HandleFunc("/queue/stats", h.GetQueueStats).Methods("GET")
}

func (h *RESTHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{
		"status":  "healthy",
		"service": "job-orchestrator",
		"time":    time.Now().Unix(),
	})
}

func (h *RESTHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	stats := h.orchestrator.GetQueueStats()
	resourceStats := h.orchestrator.GetResourceStatus(r.Context())
	respondJSON(w, http.StatusOK, map[string]any{
		"queue":     stats,
		"resources": resourceStats,
	})
}

func (h *RESTHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req service.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}

	if req.UserID == "" || req.JobType == "" {
		respondError(w, http.StatusBadRequest, "MISSING_FIELDS", "Required fields missing")
		return
	}

	job, queuePosition, err := h.orchestrator.CreateJob(r.Context(), req.UserID, req.JobType, req.Priority, req.Payload)
	if err != nil {
		log.Printf("Failed to create job: %v", err)
		respondError(w, http.StatusInternalServerError, "CREATE_FAILED", err.Error())
		return
	}

	response := map[string]any{
		"job_id":          job.ID,
		"status":          job.Status,
		"queue_position":  queuePosition,
		"estimated_start": time.Now().Add(time.Duration(queuePosition*5) * time.Minute).Unix(),
		"created_at":      job.CreatedAt.Unix(),
	}
	respondJSON(w, http.StatusCreated, response)
}

func (h *RESTHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	job, err := h.orchestrator.GetJob(r.Context(), jobID)
	if err != nil {
		respondError(w, http.StatusNotFound, "NOT_FOUND", "Job not found")
		return
	}

	response := map[string]any{
		"job_id":     job.ID,
		"user_id":    job.UserID,
		"job_type":   job.JobType,
		"status":     job.Status,
		"priority":   job.Priority,
		"progress":   job.Progress,
		"result":     job.Result,
		"created_at": job.CreatedAt.Unix(),
	}

	if job.StartedAt != nil {
		response["started_at"] = job.StartedAt.Unix()
	}
	if job.CompletedAt != nil {
		response["completed_at"] = job.CompletedAt.Unix()
	}
	if job.ErrorMessage != "" {
		response["error_message"] = job.ErrorMessage
	}

	respondJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	status := service.JobStatus(r.URL.Query().Get("status"))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	jobs, totalCount := h.orchestrator.ListJobs(r.Context(), userID, status, page, pageSize)

	jobResponses := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		jr := map[string]any{
			"job_id":     job.ID,
			"user_id":    job.UserID,
			"job_type":   job.JobType,
			"status":     job.Status,
			"priority":   job.Priority,
			"progress":   job.Progress,
			"created_at": job.CreatedAt.Unix(),
		}
		if job.StartedAt != nil {
			jr["started_at"] = job.StartedAt.Unix()
		}
		if job.CompletedAt != nil {
			jr["completed_at"] = job.CompletedAt.Unix()
		}
		jobResponses = append(jobResponses, jr)
	}

	response := map[string]any{
		"jobs":        jobResponses,
		"total_count": totalCount,
		"page":        page,
		"page_size":   pageSize,
	}
	respondJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	if err := h.orchestrator.CancelJob(r.Context(), jobID, "User cancelled"); err != nil {
		respondError(w, http.StatusInternalServerError, "CANCEL_FAILED", err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": "Job cancelled successfully",
	})
}

func (h *RESTHandler) CreateValidationPipeline(w http.ResponseWriter, r *http.Request) {
	var req service.CreateValidationPipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}

	if req.UserID == "" || req.DatasetID == "" || req.DatasetPath == "" {
		respondError(w, http.StatusBadRequest, "MISSING_FIELDS", "Required fields missing")
		return
	}

	pipeline, err := h.orchestrator.CreateValidationPipeline(r.Context(), req)
	if err != nil {
		log.Printf("Failed to create validation pipeline: %v", err)
		respondError(w, http.StatusInternalServerError, "CREATE_FAILED", err.Error())
		return
	}

	stageDetails := make([]map[string]any, 0, len(pipeline.Stages))
	for _, stage := range pipeline.Stages {
		stageDetails = append(stageDetails, map[string]any{
			"stage":          stage.Name,
			"status":         stage.Status,
			"progress":       stage.Progress,
			"estimated_time": stage.EstimatedTime,
		})
	}

	response := map[string]any{
		"pipeline_id":          pipeline.ID,
		"status":               pipeline.Status,
		"job_ids":              pipeline.JobIDs,
		"stage_details":        stageDetails,
		"created_at":           pipeline.CreatedAt.Unix(),
		"estimated_completion": pipeline.EstimatedCompletion.Unix(),
	}
	respondJSON(w, http.StatusCreated, response)
}

func (h *RESTHandler) CreateFullPipeline(w http.ResponseWriter, r *http.Request) {
	var req service.CreateFullPipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}

	if req.UserID == "" || req.DatasetID == "" || req.DatasetPath == "" {
		respondError(w, http.StatusBadRequest, "MISSING_FIELDS", "Required fields missing")
		return
	}

	pipeline, err := h.orchestrator.CreateFullPipeline(r.Context(), req)
	if err != nil {
		log.Printf("Failed to create full pipeline: %v", err)
		respondError(w, http.StatusInternalServerError, "CREATE_FAILED", err.Error())
		return
	}

	stageDetails := make([]map[string]any, 0, len(pipeline.Stages))
	for _, stage := range pipeline.Stages {
		stageDetails = append(stageDetails, map[string]any{
			"stage":          stage.Name,
			"status":         stage.Status,
			"progress":       stage.Progress,
			"estimated_time": stage.EstimatedTime,
		})
	}

	response := map[string]any{
		"pipeline_id":          pipeline.ID,
		"status":               pipeline.Status,
		"job_ids":              pipeline.JobIDs,
		"stage_details":        stageDetails,
		"created_at":           pipeline.CreatedAt.Unix(),
		"estimated_completion": pipeline.EstimatedCompletion.Unix(),
	}
	respondJSON(w, http.StatusCreated, response)
}

func (h *RESTHandler) GetPipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineID := vars["id"]

	pipeline, err := h.orchestrator.GetPipeline(r.Context(), pipelineID)
	if err != nil {
		respondError(w, http.StatusNotFound, "NOT_FOUND", "Pipeline not found")
		return
	}

	stageDetails := make([]map[string]any, 0, len(pipeline.Stages))
	for _, stage := range pipeline.Stages {
		sd := map[string]any{
			"stage":          stage.Name,
			"status":         stage.Status,
			"progress":       stage.Progress,
			"estimated_time": stage.EstimatedTime,
		}
		if stage.StartedAt != nil {
			sd["started_at"] = stage.StartedAt.Unix()
		}
		if stage.CompletedAt != nil {
			sd["completed_at"] = stage.CompletedAt.Unix()
		}
		stageDetails = append(stageDetails, sd)
	}

	response := map[string]any{
		"pipeline_id":          pipeline.ID,
		"status":               pipeline.Status,
		"job_ids":              pipeline.JobIDs,
		"progress":             pipeline.Progress,
		"results":              pipeline.Results,
		"stage_details":        stageDetails,
		"created_at":           pipeline.CreatedAt.Unix(),
		"estimated_completion": pipeline.EstimatedCompletion.Unix(),
	}

	if pipeline.CurrentStage != "" {
		response["current_stage"] = pipeline.CurrentStage
	}
	if pipeline.CompletedAt != nil {
		response["completed_at"] = pipeline.CompletedAt.Unix()
	}
	if pipeline.ErrorMessage != "" {
		response["error_message"] = pipeline.ErrorMessage
	}

	respondJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) GetResourceStatus(w http.ResponseWriter, r *http.Request) {
	status := h.orchestrator.GetResourceStatus(r.Context())
	respondJSON(w, http.StatusOK, status)
}

func (h *RESTHandler) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	stats := h.orchestrator.GetQueueStats()
	respondJSON(w, http.StatusOK, stats)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}
}

func respondError(w http.ResponseWriter, status int, code, message string) {
	respondJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
