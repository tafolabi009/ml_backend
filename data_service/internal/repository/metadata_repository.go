package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatasetMetadata mirrors the datasets table used by the service.
type DatasetMetadata struct {
	ID          string
	UserID      string
	Filename    string
	FileSize    int64
	FileType    string
	StoragePath string
	Status      string
	RowCount    *int64
	ColumnCount  *int32
	Description *string
	Metadata    map[string]any
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ProcessedAt *time.Time
	DeletedAt   *time.Time
}

// MetadataRepository persists dataset metadata in Postgres.
type MetadataRepository struct {
	db *pgxpool.Pool
}

func NewMetadataRepository(db *pgxpool.Pool) *MetadataRepository {
	return &MetadataRepository{db: db}
}

func (r *MetadataRepository) UpsertDataset(ctx context.Context, meta DatasetMetadata) error {
	if r == nil || r.db == nil {
		return nil
	}

	query := `
INSERT INTO datasets (
	id, user_id, filename, file_size, file_type, storage_path,
	status, row_count, column_count, description, metadata,
	created_at, updated_at, processed_at, deleted_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10, $11,
	now(), now(), $12, $13
)
ON CONFLICT (id) DO UPDATE SET
	user_id = EXCLUDED.user_id,
	filename = EXCLUDED.filename,
	file_size = EXCLUDED.file_size,
	file_type = EXCLUDED.file_type,
	storage_path = EXCLUDED.storage_path,
	status = EXCLUDED.status,
	row_count = EXCLUDED.row_count,
	column_count = EXCLUDED.column_count,
	description = EXCLUDED.description,
	metadata = EXCLUDED.metadata,
	updated_at = now(),
	processed_at = COALESCE(EXCLUDED.processed_at, datasets.processed_at),
	deleted_at = EXCLUDED.deleted_at
`
	_, err := r.db.Exec(ctx, query,
		meta.ID,
		meta.UserID,
		meta.Filename,
		meta.FileSize,
		meta.FileType,
		meta.StoragePath,
		meta.Status,
		meta.RowCount,
		meta.ColumnCount,
		meta.Description,
		meta.Metadata,
		meta.ProcessedAt,
		meta.DeletedAt,
	)
	return err
}

func (r *MetadataRepository) GetDataset(ctx context.Context, userID, datasetID string) (*DatasetMetadata, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}

	query := `
SELECT id, user_id, filename, file_size, file_type, storage_path, status,
	row_count, column_count, description, metadata,
	created_at, processed_at, deleted_at
FROM datasets
WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL
LIMIT 1`

	var meta DatasetMetadata
	var processedAt, deletedAt *time.Time
	var rowCount *int64
	var columnCount *int32
	var description *string
	var metadata map[string]any
	err := r.db.QueryRow(ctx, query, datasetID, userID).Scan(
		&meta.ID,
		&meta.UserID,
		&meta.Filename,
		&meta.FileSize,
		&meta.FileType,
		&meta.StoragePath,
		&meta.Status,
		&rowCount,
		&columnCount,
		&description,
		&metadata,
		&meta.CreatedAt,
		&processedAt,
		&deletedAt,
	)
	if err != nil {
		return nil, err
	}

	meta.RowCount = rowCount
	meta.ColumnCount = columnCount
	meta.Description = description
	meta.Metadata = metadata
	meta.ProcessedAt = processedAt
	meta.DeletedAt = deletedAt
	return &meta, nil
}

func (r *MetadataRepository) ListDatasets(ctx context.Context, userID string, limit, offset int) ([]DatasetMetadata, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}

	query := `
SELECT id, user_id, filename, file_size, file_type, storage_path, status,
	row_count, column_count, description, metadata,
	created_at, processed_at, deleted_at
FROM datasets
WHERE user_id = $1 AND deleted_at IS NULL
ORDER BY created_at DESC
LIMIT $2 OFFSET $3`

	rows, err := r.db.Query(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DatasetMetadata
	for rows.Next() {
		var meta DatasetMetadata
		var processedAt, deletedAt *time.Time
		var rowCount *int64
		var columnCount *int32
		var description *string
		var metadata map[string]any
		if err := rows.Scan(
			&meta.ID,
			&meta.UserID,
			&meta.Filename,
			&meta.FileSize,
			&meta.FileType,
			&meta.StoragePath,
			&meta.Status,
			&rowCount,
			&columnCount,
			&description,
			&metadata,
			&meta.CreatedAt,
			&processedAt,
			&deletedAt,
		); err != nil {
			return nil, err
		}
		meta.RowCount = rowCount
		meta.ColumnCount = columnCount
		meta.Description = description
		meta.Metadata = metadata
		meta.ProcessedAt = processedAt
		meta.DeletedAt = deletedAt
		result = append(result, meta)
	}
	return result, rows.Err()
}

func (r *MetadataRepository) CountDatasets(ctx context.Context, userID string) (int64, error) {
	if r == nil || r.db == nil {
		return 0, nil
	}
	var count int64
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM datasets WHERE user_id = $1 AND deleted_at IS NULL`, userID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *MetadataRepository) DeleteDataset(ctx context.Context, userID, datasetID string) error {
	if r == nil || r.db == nil {
		return nil
	}
	cmd, err := r.db.Exec(ctx, `UPDATE datasets SET deleted_at = now(), status = 'deleted', updated_at = now() WHERE id = $1 AND user_id = $2`, datasetID, userID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("dataset not found")
	}
	return nil
}

func (r *MetadataRepository) EnsureSchema(ctx context.Context) error {
	if r == nil || r.db == nil {
		return nil
	}
	_, err := r.db.Exec(ctx, `
CREATE TABLE IF NOT EXISTS datasets (
	id VARCHAR(255) PRIMARY KEY,
	user_id VARCHAR(255) NOT NULL,
	filename VARCHAR(500) NOT NULL,
	file_size BIGINT NOT NULL,
	file_type VARCHAR(100) NOT NULL,
	storage_path VARCHAR(1000),
	status VARCHAR(50) NOT NULL DEFAULT 'pending',
	row_count BIGINT,
	column_count INT,
	description TEXT,
	metadata JSONB,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	processed_at TIMESTAMP,
	deleted_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_datasets_user_id ON datasets(user_id);
CREATE INDEX IF NOT EXISTS idx_datasets_created_at ON datasets(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_datasets_deleted_at ON datasets(deleted_at) WHERE deleted_at IS NULL;
`)
	if err != nil {
		return fmt.Errorf("ensure datasets schema: %w", err)
	}
	return nil
}
