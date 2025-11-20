package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// GCSProvider implements StorageProvider for Google Cloud Storage
type GCSProvider struct {
	client     *storage.Client
	bucket     string
	bucketName string
}

// NewGCSProvider creates a new GCS storage provider
func NewGCSProvider(ctx context.Context, bucketName, credentialsPath string) (*GCSProvider, error) {
	var client *storage.Client
	var err error

	if credentialsPath != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(credentialsPath))
	} else {
		// Use default credentials (from environment or metadata server)
		client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucket := client.Bucket(bucketName)

	// Verify bucket exists and is accessible
	_, err = bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %w", bucketName, err)
	}

	return &GCSProvider{
		client:     client,
		bucket:     bucketName,
		bucketName: bucketName,
	}, nil
}

// Upload uploads a file to GCS
func (p *GCSProvider) Upload(ctx context.Context, key string, reader io.Reader, size int64, contentType string) (string, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	writer := obj.NewWriter(ctx)
	writer.ContentType = contentType
	writer.ChunkSize = 0 // Use default chunk size

	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return "", fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close GCS writer: %w", err)
	}

	// Return public URL or signed URL
	url := fmt.Sprintf("gs://%s/%s", p.bucket, key)
	return url, nil
}

// Download downloads a file from GCS
func (p *GCSProvider) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS reader: %w", err)
	}
	return reader, nil
}

// Delete deletes a file from GCS
func (p *GCSProvider) Delete(ctx context.Context, key string) error {
	obj := p.client.Bucket(p.bucket).Object(key)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete from GCS: %w", err)
	}
	return nil
}

// Exists checks if a file exists in GCS
func (p *GCSProvider) Exists(ctx context.Context, key string) (bool, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	_, err := obj.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check GCS object: %w", err)
	}
	return true, nil
}

// GetSize returns the size of a file in GCS
func (p *GCSProvider) GetSize(ctx context.Context, key string) (int64, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get GCS object attrs: %w", err)
	}
	return attrs.Size, nil
}

// GenerateSignedURL generates a signed URL for uploads
func (p *GCSProvider) GenerateSignedURL(ctx context.Context, key string, method string, expirationMinutes int) (string, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	
	opts := &storage.SignedURLOptions{
		Method:  method,
		Expires: time.Now().Add(time.Duration(expirationMinutes) * time.Minute),
		Scheme:  storage.SigningSchemeV4,
	}

	url, err := obj.SignedURL(opts)
	if err != nil {
		return "", fmt.Errorf("failed to generate signed URL: %w", err)
	}

	return url, nil
}

// GeneratePresignedUploadURL generates a presigned URL for direct upload
func (p *GCSProvider) GeneratePresignedUploadURL(ctx context.Context, key string, contentType string, expirationMinutes int) (string, map[string]string, error) {
	// For GCS, signed URLs don't use additional fields like S3
	url, err := p.GenerateSignedURL(ctx, key, "PUT", expirationMinutes)
	if err != nil {
		return "", nil, err
	}

	headers := map[string]string{
		"Content-Type": contentType,
	}

	return url, headers, nil
}

// GeneratePresignedDownloadURL generates a presigned URL for direct download
func (p *GCSProvider) GeneratePresignedDownloadURL(ctx context.Context, key string, expirationMinutes int) (string, error) {
	return p.GenerateSignedURL(ctx, key, "GET", expirationMinutes)
}

// List lists files with a given prefix
func (p *GCSProvider) List(ctx context.Context, prefix string, maxResults int) ([]string, error) {
	query := &storage.Query{
		Prefix: prefix,
	}
	if maxResults > 0 {
		query.MaxResults = maxResults
	}

	it := p.client.Bucket(p.bucket).Objects(ctx, query)
	
	keys := []string{}
	for {
		attrs, err := it.Next()
		if err == storage.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate GCS objects: %w", err)
		}
		keys = append(keys, attrs.Name)
	}

	return keys, nil
}

// Copy copies a file within GCS
func (p *GCSProvider) Copy(ctx context.Context, sourceKey, destKey string) error {
	src := p.client.Bucket(p.bucket).Object(sourceKey)
	dst := p.client.Bucket(p.bucket).Object(destKey)
	
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		return fmt.Errorf("failed to copy GCS object: %w", err)
	}
	
	return nil
}

// Close closes the GCS client
func (p *GCSProvider) Close() error {
	return p.client.Close()
}
