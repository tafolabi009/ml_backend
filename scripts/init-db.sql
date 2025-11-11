-- Initialize Synthos database schema
-- Run on first startup

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    company_id UUID,
    company_name VARCHAR(255),
    subscription_tier VARCHAR(50) DEFAULT 'free',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(50),
    s3_path VARCHAR(500),
    status VARCHAR(50) DEFAULT 'uploading',
    row_count BIGINT,
    column_count INT,
    data_quality_score INT,
    has_pii BOOLEAN,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create validations table
CREATE TABLE IF NOT EXISTS validations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'queued',
    validation_type VARCHAR(50) DEFAULT 'full',
    current_stage VARCHAR(50),
    progress_percent INT DEFAULT 0,
    target_model_size BIGINT,
    target_architecture VARCHAR(50),
    priority VARCHAR(50) DEFAULT 'standard',
    enable_warranty BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_completion TIMESTAMP,
    risk_score INT,
    collapse_detected BOOLEAN,
    collapse_type VARCHAR(100),
    predicted_accuracy DECIMAL(5,4),
    confidence_lower DECIMAL(5,4),
    confidence_upper DECIMAL(5,4),
    distribution_fidelity INT,
    correlation_preservation INT,
    diversity_retention INT,
    rare_pattern_handling INT,
    temporal_stability INT,
    semantic_coherence INT,
    results JSONB,
    error_message TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create validation_stages table
CREATE TABLE IF NOT EXISTS validation_stages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    validation_id UUID REFERENCES validations(id) ON DELETE CASCADE,
    stage_name VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    progress_percent INT DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    results JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create collapse_signatures table
CREATE TABLE IF NOT EXISTS collapse_signatures (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    validation_id UUID REFERENCES validations(id),
    collapse_type VARCHAR(100) NOT NULL,
    fingerprint JSONB NOT NULL,
    severity VARCHAR(50),
    root_cause TEXT,
    dataset_source VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create recommendations table
CREATE TABLE IF NOT EXISTS recommendations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    validation_id UUID REFERENCES validations(id) ON DELETE CASCADE,
    priority INT NOT NULL,
    category VARCHAR(50),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    current_risk_score INT,
    expected_risk_score INT,
    improvement INT,
    method VARCHAR(100),
    affected_rows BIGINT,
    estimated_time VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_datasets_user_id ON datasets(user_id);
CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(status);
CREATE INDEX IF NOT EXISTS idx_validations_user_id ON validations(user_id);
CREATE INDEX IF NOT EXISTS idx_validations_dataset_id ON validations(dataset_id);
CREATE INDEX IF NOT EXISTS idx_validations_status ON validations(status);
CREATE INDEX IF NOT EXISTS idx_validation_stages_validation_id ON validation_stages(validation_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_validation_id ON recommendations(validation_id);

-- Insert a test user (password is "Test123!")
INSERT INTO users (email, password_hash, full_name, company_name, subscription_tier)
VALUES (
    'test@synthos.ai',
    '$2a$10$K9J8xPZqVYDV5P.WzL4/.OxG7u9PxXN5Bk.bYH2VvL0nYEzF6YH.K',
    'Test User',
    'Synthos Test Co',
    'professional'
) ON CONFLICT (email) DO NOTHING;
