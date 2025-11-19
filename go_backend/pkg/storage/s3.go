package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config holds S3 configuration
type S3Config struct {
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string // Optional: for S3-compatible services
	UsePathStyle    bool   // For S3-compatible services
	EncryptionKey   string // Optional: for SSE-C
}

// S3Client wraps AWS S3 client with advanced features
type S3Client struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	config     S3Config
}

// NewS3Client creates a new S3 client with advanced configuration
func NewS3Client(ctx context.Context, cfg S3Config) (*S3Client, error) {
	// Configure AWS SDK
	var awsCfg aws.Config
	var err error

	if cfg.Endpoint != "" {
		// Custom endpoint (e.g., MinIO, LocalStack)
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               cfg.Endpoint,
				HostnameImmutable: true,
				SigningRegion:     cfg.Region,
			}, nil
		})

		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			)),
		)
	} else {
		// Standard AWS S3
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			)),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})

	// Create upload and download managers with optimized settings
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10MB parts
		u.Concurrency = 5             // 5 concurrent uploads
		u.LeavePartsOnError = false
	})

	downloader := manager.NewDownloader(s3Client, func(d *manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 10MB parts
		d.Concurrency = 5             // 5 concurrent downloads
	})

	client := &S3Client{
		client:     s3Client,
		uploader:   uploader,
		downloader: downloader,
		config:     cfg,
	}

	log.Printf("✅ S3 client initialized for bucket: %s", cfg.Bucket)
	return client, nil
}

// UploadOptions holds options for file upload
type UploadOptions struct {
	ContentType          string
	Metadata             map[string]string
	StorageClass         types.StorageClass
	ServerSideEncryption bool
	ACL                  types.ObjectCannedACL
}

// Upload uploads a file to S3 with multipart upload support
func (c *S3Client) Upload(ctx context.Context, key string, body io.Reader, opts UploadOptions) (*manager.UploadOutput, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(c.config.Bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(opts.ContentType),
		Metadata:    opts.Metadata,
	}

	// Add storage class if specified
	if opts.StorageClass != "" {
		input.StorageClass = opts.StorageClass
	}

	// Add server-side encryption
	if opts.ServerSideEncryption {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}

	// Add ACL if specified
	if opts.ACL != "" {
		input.ACL = opts.ACL
	}

	result, err := c.uploader.Upload(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to upload file to S3: %w", err)
	}

	log.Printf("✅ Uploaded file to S3: %s", key)
	return result, nil
}

// Download downloads a file from S3
func (c *S3Client) Download(ctx context.Context, key string, writer io.WriterAt) (int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	numBytes, err := c.downloader.Download(ctx, writer, input)
	if err != nil {
		return 0, fmt.Errorf("failed to download file from S3: %w", err)
	}

	log.Printf("✅ Downloaded file from S3: %s (%d bytes)", key, numBytes)
	return numBytes, nil
}

// GeneratePresignedURL generates a presigned URL for upload or download
func (c *S3Client) GeneratePresignedURL(ctx context.Context, key string, operation string, expiration time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(c.client)

	var presignedURL string
	var err error

	switch operation {
	case "GET", "download":
		request, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.config.Bucket),
			Key:    aws.String(key),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = expiration
		})
		if err != nil {
			return "", fmt.Errorf("failed to generate presigned GET URL: %w", err)
		}
		presignedURL = request.URL

	case "PUT", "upload":
		request, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.config.Bucket),
			Key:    aws.String(key),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = expiration
		})
		if err != nil {
			return "", fmt.Errorf("failed to generate presigned PUT URL: %w", err)
		}
		presignedURL = request.URL

	default:
		return "", fmt.Errorf("unsupported operation: %s", operation)
	}

	log.Printf("✅ Generated presigned URL for %s operation: %s", operation, key)
	return presignedURL, err
}

// Delete deletes a file from S3
func (c *S3Client) Delete(ctx context.Context, key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	_, err := c.client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete file from S3: %w", err)
	}

	log.Printf("✅ Deleted file from S3: %s", key)
	return nil
}

// DeleteMultiple deletes multiple files from S3
func (c *S3Client) DeleteMultiple(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	objects := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = types.ObjectIdentifier{
			Key: aws.String(key),
		}
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(c.config.Bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}

	_, err := c.client.DeleteObjects(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete multiple files from S3: %w", err)
	}

	log.Printf("✅ Deleted %d files from S3", len(keys))
	return nil
}

// Exists checks if a file exists in S3
func (c *S3Client) Exists(ctx context.Context, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	_, err := c.client.HeadObject(ctx, input)
	if err != nil {
		// Check if it's a NotFound error
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file exists: %w", err)
	}

	return true, nil
}

// GetMetadata retrieves metadata for a file
func (c *S3Client) GetMetadata(ctx context.Context, key string) (map[string]string, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}

	result, err := c.client.HeadObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get file metadata: %w", err)
	}

	return result.Metadata, nil
}

// CopyObject copies a file within S3
func (c *S3Client) CopyObject(ctx context.Context, sourceKey, destKey string) error {
	copySource := fmt.Sprintf("%s/%s", c.config.Bucket, sourceKey)

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(c.config.Bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(destKey),
	}

	_, err := c.client.CopyObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	log.Printf("✅ Copied file from %s to %s", sourceKey, destKey)
	return nil
}

// ListObjects lists objects with a given prefix
func (c *S3Client) ListObjects(ctx context.Context, prefix string, maxKeys int32) ([]types.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.config.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	}

	result, err := c.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	return result.Contents, nil
}

// SetLifecyclePolicy sets lifecycle policy for the bucket
func (c *S3Client) SetLifecyclePolicy(ctx context.Context, rules []types.LifecycleRule) error {
	input := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(c.config.Bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	}

	_, err := c.client.PutBucketLifecycleConfiguration(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to set lifecycle policy: %w", err)
	}

	log.Printf("✅ Set lifecycle policy for bucket: %s", c.config.Bucket)
	return nil
}

// CreateBucket creates a new S3 bucket if it doesn't exist
func (c *S3Client) CreateBucket(ctx context.Context) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(c.config.Bucket),
	}

	// Add location constraint if not us-east-1
	if c.config.Region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(c.config.Region),
		}
	}

	_, err := c.client.CreateBucket(ctx, input)
	if err != nil {
		// Check if bucket already exists
		var bucketAlreadyExists *types.BucketAlreadyExists
		var bucketAlreadyOwnedByYou *types.BucketAlreadyOwnedByYou
		if errors.As(err, &bucketAlreadyExists) || errors.As(err, &bucketAlreadyOwnedByYou) {
			log.Printf("Bucket %s already exists", c.config.Bucket)
			return nil
		}
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	log.Printf("✅ Created bucket: %s", c.config.Bucket)
	return nil
}

// EnableVersioning enables versioning for the bucket
func (c *S3Client) EnableVersioning(ctx context.Context) error {
	input := &s3.PutBucketVersioningInput{
		Bucket: aws.String(c.config.Bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}

	_, err := c.client.PutBucketVersioning(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to enable versioning: %w", err)
	}

	log.Printf("✅ Enabled versioning for bucket: %s", c.config.Bucket)
	return nil
}
