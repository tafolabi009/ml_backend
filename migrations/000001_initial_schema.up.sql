-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(255) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    company_id VARCHAR(255),
    company_name VARCHAR(255),
    subscription_tier VARCHAR(50) DEFAULT 'free',
    api_key VARCHAR(255) UNIQUE,
    rate_limit_tier VARCHAR(50) DEFAULT 'standard',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    email_verified BOOLEAN DEFAULT false
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_company_id ON users(company_id);
CREATE INDEX idx_users_created_at ON users(created_at DESC);

-- Create datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(100) NOT NULL,
    storage_path VARCHAR(1000),
    upload_url TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    format VARCHAR(50),
    row_count BIGINT,
    column_count INT,
    description TEXT,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_datasets_user_id ON datasets(user_id);
CREATE INDEX idx_datasets_status ON datasets(status);
CREATE INDEX idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX idx_datasets_deleted_at ON datasets(deleted_at) WHERE deleted_at IS NULL;

-- Create validations table
CREATE TABLE IF NOT EXISTS validations (
    id VARCHAR(255) PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    job_id VARCHAR(255),
    pipeline_id VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    priority VARCHAR(20) DEFAULT 'standard',
    progress FLOAT DEFAULT 0,
    current_stage VARCHAR(100),
    estimated_completion TIMESTAMP,
    diversity_score FLOAT,
    validation_score FLOAT,
    collapse_detected BOOLEAN,
    collapse_severity VARCHAR(50),
    report_url TEXT,
    certificate_url TEXT,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_validations_dataset_id ON validations(dataset_id);
CREATE INDEX idx_validations_user_id ON validations(user_id);
CREATE INDEX idx_validations_status ON validations(status);
CREATE INDEX idx_validations_created_at ON validations(created_at DESC);
CREATE INDEX idx_validations_job_id ON validations(job_id);
CREATE INDEX idx_validations_pipeline_id ON validations(pipeline_id);

-- Create jobs table (for orchestrator persistence)
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    priority INT DEFAULT 5,
    payload JSONB,
    result JSONB,
    error_message TEXT,
    queue_position INT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_start_time TIMESTAMP
);

CREATE INDEX idx_jobs_user_id ON jobs(user_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_job_type ON jobs(job_type);
CREATE INDEX idx_jobs_priority ON jobs(priority DESC);
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX idx_jobs_queue_position ON jobs(queue_position) WHERE status = 'queued';

-- Create pipelines table
CREATE TABLE IF NOT EXISTS pipelines (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_id VARCHAR(255),
    dataset_path TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'queued',
    progress FLOAT DEFAULT 0,
    current_stage VARCHAR(100),
    stages JSONB NOT NULL,
    job_ids TEXT[],
    results JSONB,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_completion TIMESTAMP
);

CREATE INDEX idx_pipelines_user_id ON pipelines(user_id);
CREATE INDEX idx_pipelines_dataset_id ON pipelines(dataset_id);
CREATE INDEX idx_pipelines_status ON pipelines(status);
CREATE INDEX idx_pipelines_created_at ON pipelines(created_at DESC);

-- Create warranties table
CREATE TABLE IF NOT EXISTS warranties (
    id VARCHAR(255) PRIMARY KEY,
    validation_id VARCHAR(255) NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'requested',
    warranty_type VARCHAR(50) NOT NULL,
    coverage_amount DECIMAL(10,2),
    premium_amount DECIMAL(10,2),
    terms JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    rejected_at TIMESTAMP,
    rejection_reason TEXT
);

CREATE INDEX idx_warranties_validation_id ON warranties(validation_id);
CREATE INDEX idx_warranties_user_id ON warranties(user_id);
CREATE INDEX idx_warranties_status ON warranties(status);
CREATE INDEX idx_warranties_created_at ON warranties(created_at DESC);

-- Create warranty_claims table
CREATE TABLE IF NOT EXISTS warranty_claims (
    id VARCHAR(255) PRIMARY KEY,
    warranty_id VARCHAR(255) NOT NULL REFERENCES warranties(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    claim_type VARCHAR(50) NOT NULL,
    claim_amount DECIMAL(10,2),
    description TEXT,
    evidence JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'submitted',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    approved_at TIMESTAMP,
    rejected_at TIMESTAMP,
    rejection_reason TEXT,
    paid_at TIMESTAMP
);

CREATE INDEX idx_warranty_claims_warranty_id ON warranty_claims(warranty_id);
CREATE INDEX idx_warranty_claims_user_id ON warranty_claims(user_id);
CREATE INDEX idx_warranty_claims_status ON warranty_claims(status);
CREATE INDEX idx_warranty_claims_created_at ON warranty_claims(created_at DESC);

-- Create audit log table
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    resource_id VARCHAR(255) NOT NULL,
    changes JSONB,
    ip_address VARCHAR(45),
    user_agent TEXT,
    trace_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX idx_audit_logs_trace_id ON audit_logs(trace_id);

-- Create API rate limiting table
CREATE TABLE IF NOT EXISTS rate_limits (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    request_count INT DEFAULT 1,
    window_start TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, endpoint, window_start)
);

CREATE INDEX idx_rate_limits_user_endpoint ON rate_limits(user_id, endpoint, window_start);
CREATE INDEX idx_rate_limits_window_start ON rate_limits(window_start);
