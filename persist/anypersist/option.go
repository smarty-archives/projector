package anypersist

import (
	"context"
	"encoding/base64"
	"math"
	"net/url"
	"strings"
	"time"
)

type Option func(*Wireup)

func Defaults() Option {
	return func(this *Wireup) {
		TimeoutAfter(time.Second * 10)(this)
		MaxRetries(math.MaxUint64)(this)
	}
}
func TimeoutAfter(httpTimeout time.Duration) Option {
	return func(this *Wireup) { this.timeout = httpTimeout }
}
func MaxRetries(max uint64) Option {
	return func(this *Wireup) { this.maxRetries = max }
}

func Choose(engine string, address *url.URL, accessKey, secretKey string,
	ctx context.Context, bucketName, pathPrefix, serviceAccountKey string) Option {

	return func(this *Wireup) {
		if engine == "gcs" {
			raw, _ := base64.StdEncoding.DecodeString(serviceAccountKey)
			GoogleCloudStorage(ctx, bucketName, pathPrefix, raw)(this)
		} else {
			S3(address, accessKey, secretKey)
		}
	}
}
func S3(address *url.URL, accessKey, secretKey string) Option {
	return func(this *Wireup) {
		this.engine = engineS3
		this.s3address = address
		this.awsAccessKey = strings.TrimSpace(accessKey)
		this.awsSecretKey = strings.TrimSpace(secretKey)
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
