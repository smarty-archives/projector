package anypersist

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/smartystreets/gcs"
	"github.com/smartystreets/projector/persist"
	"github.com/smartystreets/projector/persist/gcspersist"
	"github.com/smartystreets/projector/persist/s3persist"
)

type Wireup struct {
	engine int

	s3address    *url.URL
	awsAccessKey string
	awsSecretKey string
	timeout      time.Duration
	maxRetries   uint64

	context           context.Context
	bucketName        string
	pathPrefix        string
	serviceAccountKey []byte
}

func New(options ...Option) *Wireup {
	this := &Wireup{engine: engineUnknown}
	Defaults()(this)
	for _, option := range options {
		option(this)
	}
	return this
}

func (this *Wireup) Build() (persist.ReadWriter, error) {
	switch this.engine {
	case engineS3:
		return this.buildS3()
	case engineGCS:
		return this.buildGCS()
	default:
		return nil, errors.New("storage engine to build not specified")
	}
}

func (this *Wireup) buildS3() (persist.ReadWriter, error) {
	if this.s3address == nil {
		return nil, errors.New("no storage address specified for S3")
	} else if len(this.awsAccessKey) == 0 {
		return nil, errors.New("credentials for S3 not provided: AWS Access Key")
	} else if len(this.awsSecretKey) == 0 {
		return nil, errors.New("credentials for S3 not provided: AWS Secret Key")
	}

	var httpClient persist.HTTPClient
	httpClient = this.buildHTTPClient()
	httpClient = this.appendRetryClient(httpClient)
	engine := s3persist.NewStorage(this.s3address, this.awsAccessKey, this.awsSecretKey, httpClient)

	return engine, nil
}
func (this *Wireup) buildGCS() (persist.ReadWriter, error) {
	if len(this.bucketName) == 0 {
		return nil, errors.New("no target bucket specified for Google Cloud Storage")
	} else if len(this.serviceAccountKey) == 0 {
		return nil, errors.New("credentials for Google Cloud Storage not provided: Service Account Key")
	}

	credentials, err := gcs.ParseCredentialsFromJSON(this.serviceAccountKey)
	if err != nil {
		return nil, err
	}

	log.Printf("[INFO] Building gcs storage for bucket [%s] and path prefix [%s].", this.bucketName, this.pathPrefix)
	return gcspersist.NewReadWriter(func() gcspersist.StorageSettings {
		return gcspersist.StorageSettings{
			HTTPClient:  this.appendRetryClient(this.buildHTTPClient()),
			BucketName:  this.bucketName,
			PathPrefix:  this.pathPrefix,
			Context:     this.context,
			Credentials: credentials,
		}
	}), nil
}

func (this *Wireup) buildHTTPClient() persist.HTTPClient {
	return &http.Client{Timeout: this.timeout}
}
func (this *Wireup) appendRetryClient(client persist.HTTPClient) persist.HTTPClient {
	if this.maxRetries == 0 {
		return client
	}

	client = s3persist.NewGetRetryClient(client, int(this.maxRetries))
	client = s3persist.NewPutRetryClient(client, int(this.maxRetries))
	return client
}

const (
	engineUnknown int = iota
	engineS3
	engineGCS
)
