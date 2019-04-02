package anypersist

import (
	"context"
	"net/url"
	"strings"
	"time"
)

type Option func(*Wireup)

func S3(address *url.URL, accessKey, secretKey string) Option {
	return func(this *Wireup) {
		this.engine = engineS3
		this.s3address = address
		this.awsAccessKey = strings.TrimSpace(accessKey)
		this.awsSecretKey = strings.TrimSpace(secretKey)
	}
}
func TimeoutAfter(httpTimeout time.Duration) Option {
	return func(this *Wireup) {
		this.timeout = httpTimeout
	}
}
func MaxRetries(max uint64) Option {
	return func(this *Wireup) {
		this.maxRetries = max
	}
}

func GoogleCloudStorage(ctx context.Context, bucketName, pathPrefix string, serviceAccountKey []byte) Option {
	if ctx == nil {
		ctx = context.Background()
	}

	return func(this *Wireup) {
		this.engine = engineGCS
		this.context = ctx
		this.bucketName = strings.TrimSpace(bucketName)
		this.pathPrefix = strings.TrimSpace(pathPrefix)
		this.serviceAccountKey = serviceAccountKey
	}
}
