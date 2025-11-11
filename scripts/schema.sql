-- Synthos Database Schema
-- PostgreSQL 15+

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    company_id VARCHAR(50) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    subscription_tier VARCHAR(50) DEFAULT 'free',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_company_id ON users(company_id);

-- Datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'uploading',
    s3_path TEXT,
    row_count BIGINT,
    column_count INTEGER,
    description TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP
);

CREATE INDEX idx_datasets_user_id ON datasets(user_id);
CREATE INDEX idx_datasets_status ON datasets(status);

-- Validations table
CREATE TABLE IF NOT EXISTS validations (
    id VARCHAR(50) PRIMARY KEY,
    dataset_id VARCHAR(50) NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    user_id VARCHAR(50) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'queued',
    risk_score INTEGER,
    risk_level VARCHAR(50),
    recommendation VARCHAR(255),
    warranty_eligible BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_completion TIMESTAMP,
    error_message TEXT
);

CREATE INDEX idx_validations_user_id ON validations(user_id);
CREATE INDEX idx_validations_dataset_id ON validations(dataset_id);
CREATE INDEX idx_validations_status ON validations(status);

-- Validation results table (stores detailed results)
CREATE TABLE IF NOT EXISTS validation_results (
    id SERIAL PRIMARY KEY,
    validation_id VARCHAR(50) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
    predicted_accuracy DECIMAL(5, 4),
    confidence_interval_lower DECIMAL(5, 4),
    confidence_interval_upper DECIMAL(5, 4),
    confidence_level DECIMAL(5, 4),
    collapse_probability DECIMAL(5, 4),
    dimensions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_validation_results_validation_id ON validation_results(validation_id);

-- Collapse details table
CREATE TABLE IF NOT EXISTS collapse_details (
    id SERIAL PRIMARY KEY,
    validation_id VARCHAR(50) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
    collapse_detected BOOLEAN NOT NULL,
    collapse_type VARCHAR(255),
    severity VARCHAR(50),
    affected_dimensions JSONB,
    problematic_regions JSONB,
    root_causes JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_collapse_details_validation_id ON collapse_details(validation_id);

-- Recommendations table
CREATE TABLE IF NOT EXISTS recommendations (
    id SERIAL PRIMARY KEY,
    validation_id VARCHAR(50) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL,
    category VARCHAR(100) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    current_risk_score INTEGER,
    expected_risk_score INTEGER,
    improvement INTEGER,
    method VARCHAR(100),
    affected_rows BIGINT,
    estimated_time VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recommendations_validation_id ON recommendations(validation_id);

-- Warranties table
CREATE TABLE IF NOT EXISTS warranties (
    id VARCHAR(50) PRIMARY KEY,
    validation_id VARCHAR(50) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
    user_id VARCHAR(50) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(50) DEFAULT 'pending',
    warranty_type VARCHAR(50) NOT NULL,
    coverage_amount DECIMAL(12, 2),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    terms TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    rejected_at TIMESTAMP,
    rejection_reason TEXT
);

CREATE INDEX idx_warranties_user_id ON warranties(user_id);
CREATE INDEX idx_warranties_validation_id ON warranties(validation_id);
CREATE INDEX idx_warranties_status ON warranties(status);

-- Warranty claims table
CREATE TABLE IF NOT EXISTS warranty_claims (
    id VARCHAR(50) PRIMARY KEY,
    warranty_id VARCHAR(50) NOT NULL REFERENCES warranties(id) ON DELETE CASCADE,
    user_id VARCHAR(50) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    claim_type VARCHAR(50) NOT NULL,
    claim_amount DECIMAL(12, 2),
    description TEXT,
    evidence_urls JSONB,
    status VARCHAR(50) DEFAULT 'submitted',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    resolved_at TIMESTAMP,
    resolution TEXT
);

CREATE INDEX idx_warranty_claims_warranty_id ON warranty_claims(warranty_id);
CREATE INDEX idx_warranty_claims_user_id ON warranty_claims(user_id);
CREATE INDEX idx_warranty_claims_status ON warranty_claims(status);

-- Jobs table (for job orchestrator)
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'queued',
    priority INTEGER DEFAULT 5,
    payload JSONB,
    result JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3
);

CREATE INDEX idx_jobs_user_id ON jobs(user_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_priority ON jobs(priority);
CREATE INDEX idx_jobs_job_type ON jobs(job_type);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for users table
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create initial admin user (password: admin123)
INSERT INTO users (id, email, password_hash, full_name, company_id, company_name, subscription_tier)
VALUES (
    'usr_admin',
    'admin@synthos.ai',
    '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy',
    'Admin User',
    'cmp_synthos',
    'Synthos AI',
    'enterprise'
) ON CONFLICT (id) DO NOTHING;
