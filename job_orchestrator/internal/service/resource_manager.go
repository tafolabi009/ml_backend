package service

import (
	"log"
	"runtime"
	"sync"
	"time"
)

// ResourceAllocation represents allocated resources for a job
type ResourceAllocation struct {
	JobID       string
	CPUs        int
	MemoryMB    int
	GPUs        int
	AllocatedAt time.Time
}

// ResourceManager manages system resources
type ResourceManager struct {
	mu sync.RWMutex

	// Resource limits
	totalWorkers  int
	totalCPUs     int
	totalMemoryMB int
	totalGPUs     int

	// Current usage
	activeWorkers   int
	allocatedCPUs   int
	allocatedMemory int
	allocatedGPUs   int

	// Allocations tracking
	allocations map[string]*ResourceAllocation

	// Metrics
	totalJobsProcessed int
	startTime          time.Time
}

// NewResourceManager creates a new resource manager
func NewResourceManager(workers int) *ResourceManager {
	numCPU := runtime.NumCPU()

	// Get system memory (simplified - in production use actual system info)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	totalMemoryMB := int(m.Sys / 1024 / 1024)

	// Detect GPUs (simplified - in production use nvidia-smi or similar)
	totalGPUs := detectGPUs()

	rm := &ResourceManager{
		totalWorkers:  workers,
		totalCPUs:     numCPU,
		totalMemoryMB: totalMemoryMB,
		totalGPUs:     totalGPUs,
		allocations:   make(map[string]*ResourceAllocation),
		startTime:     time.Now(),
	}

	log.Printf("Resource Manager initialized: Workers=%d, CPUs=%d, Memory=%dMB, GPUs=%d",
		workers, numCPU, totalMemoryMB, totalGPUs)

	return rm
}

// CanAcceptJob checks if system can accept a new job
func (rm *ResourceManager) CanAcceptJob() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Check if we have available workers
	if rm.activeWorkers >= rm.totalWorkers {
		return false
	}

	// Check CPU and memory headroom (keep 20% reserve)
	cpuAvailable := float64(rm.totalCPUs-rm.allocatedCPUs) / float64(rm.totalCPUs)
	memoryAvailable := float64(rm.totalMemoryMB-rm.allocatedMemory) / float64(rm.totalMemoryMB)

	return cpuAvailable > 0.2 || memoryAvailable > 0.2
}

// AllocateResources allocates resources for a job
func (rm *ResourceManager) AllocateResources(jobID string, cpus, memoryMB, gpus int) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if resources are available
	if rm.activeWorkers >= rm.totalWorkers {
		log.Printf("Cannot allocate resources for job %s: no available workers", jobID)
		return false
	}

	if rm.allocatedCPUs+cpus > rm.totalCPUs {
		log.Printf("Cannot allocate resources for job %s: insufficient CPUs", jobID)
		return false
	}

	if rm.allocatedMemory+memoryMB > rm.totalMemoryMB {
		log.Printf("Cannot allocate resources for job %s: insufficient memory", jobID)
		return false
	}

	if gpus > 0 && rm.allocatedGPUs+gpus > rm.totalGPUs {
		log.Printf("Cannot allocate resources for job %s: insufficient GPUs", jobID)
		return false
	}

	// Allocate resources
	allocation := &ResourceAllocation{
		JobID:       jobID,
		CPUs:        cpus,
		MemoryMB:    memoryMB,
		GPUs:        gpus,
		AllocatedAt: time.Now(),
	}

	rm.allocations[jobID] = allocation
	rm.activeWorkers++
	rm.allocatedCPUs += cpus
	rm.allocatedMemory += memoryMB
	rm.allocatedGPUs += gpus

	log.Printf("Allocated resources for job %s: CPUs=%d, Memory=%dMB, GPUs=%d", jobID, cpus, memoryMB, gpus)
	return true
}

// ReleaseResources releases resources for a completed job
func (rm *ResourceManager) ReleaseResources(jobID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	allocation, exists := rm.allocations[jobID]
	if !exists {
		return
	}

	rm.activeWorkers--
	rm.allocatedCPUs -= allocation.CPUs
	rm.allocatedMemory -= allocation.MemoryMB
	rm.allocatedGPUs -= allocation.GPUs
	rm.totalJobsProcessed++

	delete(rm.allocations, jobID)

	log.Printf("Released resources for job %s", jobID)
}

// GetStats returns resource statistics
func (rm *ResourceManager) GetStats() map[string]any {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	cpuUsage := 0.0
	if rm.totalCPUs > 0 {
		cpuUsage = float64(rm.allocatedCPUs) / float64(rm.totalCPUs) * 100
	}

	memoryUsage := 0.0
	if rm.totalMemoryMB > 0 {
		memoryUsage = float64(rm.allocatedMemory) / float64(rm.totalMemoryMB) * 100
	}

	return map[string]any{
		"total_workers":        rm.totalWorkers,
		"active_workers":       rm.activeWorkers,
		"idle_workers":         rm.totalWorkers - rm.activeWorkers,
		"total_cpus":           rm.totalCPUs,
		"allocated_cpus":       rm.allocatedCPUs,
		"cpu_usage":            cpuUsage,
		"total_memory_mb":      rm.totalMemoryMB,
		"allocated_memory_mb":  rm.allocatedMemory,
		"memory_usage":         memoryUsage,
		"total_gpus":           rm.totalGPUs,
		"gpu_available":        rm.totalGPUs - rm.allocatedGPUs,
		"gpu_in_use":           rm.allocatedGPUs,
		"total_jobs_processed": rm.totalJobsProcessed,
		"uptime_seconds":       int(time.Since(rm.startTime).Seconds()),
	}
}

// GetAllocation returns allocation info for a job
func (rm *ResourceManager) GetAllocation(jobID string) (*ResourceAllocation, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	allocation, exists := rm.allocations[jobID]
	return allocation, exists
}

// detectGPUs detects available GPUs (simplified)
func detectGPUs() int {
	// In production, this would use nvidia-smi or similar
	// For now, return 0 (no GPU) or check environment variable
	// This is a placeholder
	return 2 // Assume 2 GPUs for development
}
