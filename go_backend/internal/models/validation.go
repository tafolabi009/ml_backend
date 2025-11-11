package models

import (
	"time"
)

// Validation represents a validation job
type Validation struct {
	ID                 string     `json:"validation_id" db:"id"`
	DatasetID          string     `json:"dataset_id" db:"dataset_id"`
	UserID             string     `json:"user_id" db:"user_id"`
	Status             string     `json:"status" db:"status"` // queued, running, completed, failed
	RiskScore          *int       `json:"risk_score,omitempty" db:"risk_score"`
	RiskLevel          *string    `json:"risk_level,omitempty" db:"risk_level"`
	Recommendation     *string    `json:"recommendation,omitempty" db:"recommendation"`
	WarrantyEligible   *bool      `json:"warranty_eligible,omitempty" db:"warranty_eligible"`
	CreatedAt          time.Time  `json:"created_at" db:"created_at"`
	StartedAt          *time.Time `json:"started_at,omitempty" db:"started_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty" db:"completed_at"`
	EstimatedCompletion time.Time  `json:"estimated_completion" db:"estimated_completion"`
}

// CreateValidationRequest is the request to create a new validation
type CreateValidationRequest struct {
	DatasetID      string            `json:"dataset_id" binding:"required"`
	ValidationType string            `json:"validation_type" binding:"required"`
	Options        ValidationOptions `json:"options"`
}

// ValidationOptions contains optional configuration
type ValidationOptions struct {
	TargetModelSize   string `json:"target_model_size"`
	TargetArchitecture string `json:"target_architecture"`
	Priority          string `json:"priority"` // standard, express
	EnableWarranty    bool   `json:"enable_warranty"`
}

// CreateValidationResponse contains the created validation info
type CreateValidationResponse struct {
	ValidationID         string            `json:"validation_id"`
	DatasetID            string            `json:"dataset_id"`
	Status               string            `json:"status"`
	EstimatedCompletion  time.Time         `json:"estimated_completion"`
	EstimatedCost        int               `json:"estimated_cost"`
	Stages               []ValidationStage `json:"stages"`
}

// ValidationStage represents a stage in the validation pipeline
type ValidationStage struct {
	Stage             string  `json:"stage"`
	Status            string  `json:"status"`
	Progress          float64 `json:"progress"`
	EstimatedDuration int     `json:"estimated_duration"` // seconds
}

// ValidationResults contains the complete validation results
type ValidationResults struct {
	RiskScore            int                   `json:"risk_score"`
	RiskLevel            string                `json:"risk_level"`
	PredictedPerformance PredictedPerformance  `json:"predicted_performance"`
	CollapseProbability  float64               `json:"collapse_probability"`
	Dimensions           map[string]int        `json:"dimensions"`
	Recommendation       string                `json:"recommendation"`
	WarrantyEligible     bool                  `json:"warranty_eligible"`
}

// PredictedPerformance contains ML model performance predictions
type PredictedPerformance struct {
	Accuracy           float64   `json:"accuracy"`
	ConfidenceInterval []float64 `json:"confidence_interval"`
	ConfidenceLevel    float64   `json:"confidence_level"`
}

// ValidationListResponse is the paginated list of validations
type ValidationListResponse struct {
	Validations []Validation `json:"validations"`
	Pagination  Pagination   `json:"pagination"`
}

// CollapseDetails contains detailed collapse analysis
type CollapseDetails struct {
	ValidationID       string              `json:"validation_id"`
	CollapseDetected   bool                `json:"collapse_detected"`
	CollapseType       string              `json:"collapse_type"`
	Severity           string              `json:"severity"`
	AffectedDimensions []AffectedDimension `json:"affected_dimensions"`
	ProblematicRegions []ProblematicRegion `json:"problematic_regions"`
	RootCauses         []RootCause         `json:"root_causes"`
}

// AffectedDimension represents a failing dimension
type AffectedDimension struct {
	Dimension string `json:"dimension"`
	Score     int    `json:"score"`
	Threshold int    `json:"threshold"`
	Impact    string `json:"impact"`
}

// ProblematicRegion identifies specific data issues
type ProblematicRegion struct {
	RegionID        string   `json:"region_id"`
	RowRange        []int64  `json:"row_range"`
	Issue           string   `json:"issue"`
	ImpactScore     int      `json:"impact_score"`
	AffectedColumns []string `json:"affected_columns"`
}

// RootCause explains why collapse occurred
type RootCause struct {
	Cause       string  `json:"cause"`
	Percentage  float64 `json:"percentage"`
	Description string  `json:"description"`
}

// Recommendations contains actionable fix suggestions
type Recommendations struct {
	ValidationID   string           `json:"validation_id"`
	Recommendations []Recommendation `json:"recommendations"`
	CombinedImpact  CombinedImpact   `json:"combined_impact"`
}

// Recommendation is a single actionable fix
type Recommendation struct {
	Priority       int            `json:"priority"`
	Category       string         `json:"category"`
	Title          string         `json:"title"`
	Description    string         `json:"description"`
	Impact         RecommendationImpact `json:"impact"`
	Implementation Implementation `json:"implementation"`
}

// RecommendationImpact shows before/after scores
type RecommendationImpact struct {
	CurrentRiskScore  int `json:"current_risk_score"`
	ExpectedRiskScore int `json:"expected_risk_score"`
	Improvement       int `json:"improvement"`
}

// Implementation contains execution details
type Implementation struct {
	Method        string `json:"method"`
	AffectedRows  int64  `json:"affected_rows"`
	EstimatedTime string `json:"estimated_time"`
}

// CombinedImpact shows total improvement if all recommendations applied
type CombinedImpact struct {
	CurrentRiskScore  int    `json:"current_risk_score"`
	ExpectedRiskScore int    `json:"expected_risk_score"`
	TotalImprovement  int    `json:"total_improvement"`
	EstimatedTime     string `json:"estimated_time"`
}
