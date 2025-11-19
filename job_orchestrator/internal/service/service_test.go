package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tafolabi009/backend/job_orchestrator/internal/service"
)

// TestResourceManager tests resource allocation and release
func TestResourceManager(t *testing.T) {
	rm := service.NewResourceManager(8, 16384, []string{"GPU:0", "GPU:1"})

	t.Run("AllocateResources", func(t *testing.T) {
		req := service.ResourceRequirements{
			CPUCores: 2,
			MemoryMB: 4096,
			GPUCount: 1,
		}

		allocation, err := rm.AllocateResources(req)
		require.NoError(t, err)
		assert.NotEmpty(t, allocation.AllocationID)
		assert.Len(t, allocation.GPUDevices, 1)
	})

	t.Run("ReleaseResources", func(t *testing.T) {
		req := service.ResourceRequirements{
			CPUCores: 2,
			MemoryMB: 4096,
			GPUCount: 1,
		}

		allocation, err := rm.AllocateResources(req)
		require.NoError(t, err)

		err = rm.ReleaseResources(allocation.AllocationID)
		assert.NoError(t, err)
	})

	t.Run("ResourceExhaustion", func(t *testing.T) {
		// Allocate all resources
		req := service.ResourceRequirements{
			CPUCores: 8,
			MemoryMB: 16384,
			GPUCount: 2,
		}

		_, err := rm.AllocateResources(req)
		require.NoError(t, err)

		// Try to allocate more - should fail
		req2 := service.ResourceRequirements{
			CPUCores: 1,
			MemoryMB: 1024,
			GPUCount: 0,
		}

		_, err = rm.AllocateResources(req2)
		assert.Error(t, err)
	})
}

// TestPipelineManager tests pipeline lifecycle
func TestPipelineManager(t *testing.T) {
	// Mock orchestrator service
	mockOrch := &service.OrchestratorService{
		// Initialize with test configuration
	}

	pm := service.NewPipelineManager(mockOrch)

	t.Run("CreateValidationPipeline", func(t *testing.T) {
		ctx := context.Background()
		pipelineID, err := pm.CreateValidationPipeline(ctx, "test_dataset_001")

		require.NoError(t, err)
		assert.NotEmpty(t, pipelineID)
	})

	t.Run("GetPipelineStatus", func(t *testing.T) {
		ctx := context.Background()
		pipelineID, err := pm.CreateValidationPipeline(ctx, "test_dataset_002")
		require.NoError(t, err)

		status, err := pm.GetPipelineStatus(pipelineID)
		require.NoError(t, err)
		assert.Equal(t, "queued", status.Status)
	})

	t.Run("PipelineExecution", func(t *testing.T) {
		ctx := context.Background()
		pipelineID, err := pm.CreateValidationPipeline(ctx, "test_dataset_003")
		require.NoError(t, err)

		// Wait for pipeline to start
		time.Sleep(100 * time.Millisecond)

		status, err := pm.GetPipelineStatus(pipelineID)
		require.NoError(t, err)
		assert.Contains(t, []string{"queued", "running", "completed"}, status.Status)
	})
}

// TestOrchestratorService tests orchestrator service operations
func TestOrchestratorService(t *testing.T) {
	// Initialize test orchestrator
	config := service.OrchestratorConfig{
		ValidationServiceAddr: "localhost:50051",
		CollapseServiceAddr:   "localhost:50052",
		DataServiceAddr:       "localhost:50054",
		WorkerPoolSize:        2,
	}

	ctx := context.Background()
	orch, err := service.NewOrchestratorService(ctx, config)
	require.NoError(t, err)
	defer orch.Close()

	t.Run("CreateJob", func(t *testing.T) {
		job := &service.Job{
			Type:      "validation",
			DatasetID: "ds_test123",
			Priority:  1,
		}

		jobID, err := orch.CreateJob(ctx, job)
		require.NoError(t, err)
		assert.NotEmpty(t, jobID)
	})

	t.Run("GetJobStatus", func(t *testing.T) {
		job := &service.Job{
			Type:      "validation",
			DatasetID: "ds_test456",
			Priority:  1,
		}

		jobID, err := orch.CreateJob(ctx, job)
		require.NoError(t, err)

		status, err := orch.GetJobStatus(jobID)
		require.NoError(t, err)
		assert.Contains(t, []string{"queued", "running", "completed", "failed"}, status)
	})

	t.Run("CancelJob", func(t *testing.T) {
		job := &service.Job{
			Type:      "validation",
			DatasetID: "ds_test789",
			Priority:  1,
		}

		jobID, err := orch.CreateJob(ctx, job)
		require.NoError(t, err)

		err = orch.CancelJob(jobID)
		assert.NoError(t, err)
	})
}

// Benchmark tests for performance validation
func BenchmarkResourceAllocation(b *testing.B) {
	rm := service.NewResourceManager(16, 32768, []string{"GPU:0", "GPU:1", "GPU:2", "GPU:3"})

	req := service.ResourceRequirements{
		CPUCores: 2,
		MemoryMB: 4096,
		GPUCount: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocation, err := rm.AllocateResources(req)
		if err == nil {
			_ = rm.ReleaseResources(allocation.AllocationID)
		}
	}
}

func BenchmarkJobCreation(b *testing.B) {
	config := service.OrchestratorConfig{
		ValidationServiceAddr: "localhost:50051",
		CollapseServiceAddr:   "localhost:50052",
		DataServiceAddr:       "localhost:50054",
		WorkerPoolSize:        10,
	}

	ctx := context.Background()
	orch, _ := service.NewOrchestratorService(ctx, config)
	defer orch.Close()

	job := &service.Job{
		Type:      "validation",
		DatasetID: "ds_bench",
		Priority:  1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = orch.CreateJob(ctx, job)
	}
}
