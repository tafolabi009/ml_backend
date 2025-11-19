package service

import (
	"context"
	"log"

	"github.com/tafolabi009/backend/proto/orchestrator"
)

// OrchestratorServer implements the JobOrchestrator gRPC service
type OrchestratorServer struct {
	orchestrator.UnimplementedJobOrchestratorServer
	service *OrchestratorService
}

// NewOrchestratorServer creates a new orchestrator gRPC server
func NewOrchestratorServer(service *OrchestratorService) *OrchestratorServer {
	return &OrchestratorServer{
		service: service,
	}
}

// CreateJob creates a new job
func (s *OrchestratorServer) CreateJob(ctx context.Context, req *orchestrator.CreateJobRequest) (*orchestrator.CreateJobResponse, error) {
	log.Printf("CreateJob request: user_id=%s, job_type=%s, priority=%d", req.UserId, req.JobType, req.Priority)

	job, queuePosition, err := s.service.CreateJob(ctx, req.UserId, req.JobType, req.Priority, req.Payload)
	if err != nil {
		return nil, err
	}

	return &orchestrator.CreateJobResponse{
		JobId:              job.ID,
		Status:             string(job.Status),
		EstimatedStartTime: job.CreatedAt.Unix() + int64(queuePosition*60), // Estimate 1 min per queued job
		QueuePosition:      int32(queuePosition),
	}, nil
}

// GetJobStatus retrieves job status
func (s *OrchestratorServer) GetJobStatus(ctx context.Context, req *orchestrator.GetJobStatusRequest) (*orchestrator.GetJobStatusResponse, error) {
	log.Printf("GetJobStatus request: job_id=%s", req.JobId)

	job, err := s.service.GetJob(ctx, req.JobId)
	if err != nil {
		return nil, err
	}

	response := &orchestrator.GetJobStatusResponse{
		JobId:              job.ID,
		Status:             string(job.Status),
		JobType:            job.JobType,
		Priority:           job.Priority,
		Result:             job.Result,
		ErrorMessage:       job.ErrorMessage,
		CreatedAt:          job.CreatedAt.Unix(),
		ProgressPercentage: job.Progress,
	}

	if job.StartedAt != nil {
		response.StartedAt = job.StartedAt.Unix()
	}

	if job.CompletedAt != nil {
		response.CompletedAt = job.CompletedAt.Unix()
	}

	return response, nil
}

// CancelJob cancels a job
func (s *OrchestratorServer) CancelJob(ctx context.Context, req *orchestrator.CancelJobRequest) (*orchestrator.CancelJobResponse, error) {
	log.Printf("CancelJob request: job_id=%s, reason=%s", req.JobId, req.Reason)

	err := s.service.CancelJob(ctx, req.JobId, req.Reason)
	if err != nil {
		return &orchestrator.CancelJobResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &orchestrator.CancelJobResponse{
		Success: true,
		Message: "Job cancelled successfully",
	}, nil
}

// ListJobs lists jobs for a user
func (s *OrchestratorServer) ListJobs(ctx context.Context, req *orchestrator.ListJobsRequest) (*orchestrator.ListJobsResponse, error) {
	log.Printf("ListJobs request: user_id=%s, status_filter=%s", req.UserId, req.StatusFilter)

	page := int(req.Page)
	if page < 1 {
		page = 1
	}

	pageSize := int(req.PageSize)
	if pageSize < 1 {
		pageSize = 20
	}

	statusFilter := JobStatus(req.StatusFilter)
	jobs, totalCount := s.service.ListJobs(ctx, req.UserId, statusFilter, page, pageSize)

	jobInfos := make([]*orchestrator.JobInfo, 0, len(jobs))
	for _, job := range jobs {
		jobInfo := &orchestrator.JobInfo{
			JobId:     job.ID,
			JobType:   job.JobType,
			Status:    string(job.Status),
			Priority:  job.Priority,
			CreatedAt: job.CreatedAt.Unix(),
		}

		if job.StartedAt != nil {
			jobInfo.StartedAt = job.StartedAt.Unix()
		}

		if job.CompletedAt != nil {
			jobInfo.CompletedAt = job.CompletedAt.Unix()
		}

		jobInfos = append(jobInfos, jobInfo)
	}

	return &orchestrator.ListJobsResponse{
		Jobs:       jobInfos,
		TotalCount: int32(totalCount),
	}, nil
}

// UpdateJobStatus updates job status (internal use)
func (s *OrchestratorServer) UpdateJobStatus(ctx context.Context, req *orchestrator.UpdateJobStatusRequest) (*orchestrator.UpdateJobStatusResponse, error) {
	log.Printf("UpdateJobStatus request: job_id=%s, status=%s", req.JobId, req.Status)

	err := s.service.queue.UpdateJobStatus(
		req.JobId,
		JobStatus(req.Status),
		req.Result,
		req.ErrorMessage,
		req.ProgressPercentage,
	)

	if err != nil {
		return &orchestrator.UpdateJobStatusResponse{Success: false}, err
	}

	return &orchestrator.UpdateJobStatusResponse{Success: true}, nil
}
