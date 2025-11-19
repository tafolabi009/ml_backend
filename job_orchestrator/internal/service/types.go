package service

// CreateValidationPipelineRequest represents a request to create a validation pipeline
type CreateValidationPipelineRequest struct {
	UserID           string            `json:"user_id"`
	DatasetID        string            `json:"dataset_id"`
	DatasetPath      string            `json:"dataset_path"`
	DataFormat       string            `json:"data_format"`
	Priority         int32             `json:"priority"`
	EnableValidation bool              `json:"enable_validation"`
	Options          PipelineOptions   `json:"options"`
	Metadata         map[string]string `json:"metadata"`
}

// CreateFullPipelineRequest represents a request to create a full pipeline
type CreateFullPipelineRequest struct {
	UserID                string            `json:"user_id"`
	DatasetID             string            `json:"dataset_id"`
	DatasetPath           string            `json:"dataset_path"`
	DataFormat            string            `json:"data_format"`
	Priority              int32             `json:"priority"`
	EnableValidation      bool              `json:"enable_validation"`
	EnableCollapse        bool              `json:"enable_collapse"`
	EnableRecommendations bool              `json:"enable_recommendations"`
	Options               PipelineOptions   `json:"options"`
	Metadata              map[string]string `json:"metadata"`
}

// PipelineOptions contains configuration for pipeline execution
type PipelineOptions struct {
	NumEpochs              int32    `json:"num_epochs"`
	BatchSize              int32    `json:"batch_size"`
	LearningRate           float32  `json:"learning_rate"`
	UseMultiGPU            bool     `json:"use_multi_gpu"`
	NumGPUs                int32    `json:"num_gpus"`
	EnableAdvancedAnalysis bool     `json:"enable_advanced_analysis"`
	TargetColumns          []string `json:"target_columns"`
}

// CreateJobRequest represents a job creation request
type CreateJobRequest struct {
	UserID      string            `json:"user_id"`
	JobType     string            `json:"job_type"`
	Priority    int32             `json:"priority"`
	Payload     map[string]string `json:"payload"`
	DatasetID   string            `json:"dataset_id,omitempty"`
	DatasetPath string            `json:"dataset_path,omitempty"`
}
