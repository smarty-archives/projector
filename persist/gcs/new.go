package gcs

import (
	"encoding/base64"
	"errors"
	"net/http"
	"time"
)

func NewRequest(method string, options ...Option) (*http.Request, error) {
	input := newModel(method, options)
	if err := input.validate(); err != nil {
		return nil, err
	} else {
		return input.buildRequest()
	}
}

const (
	GET = "GET"
	PUT = "PUT"
)

var (
	ErrMissingHTTPMethod      = errors.New("missing HTTP method")
	ErrUnrecognizedHTTPMethod = errors.New("unrecognized HTTP method")
	ErrBucketMissing          = errors.New("bucket is required")
	ErrResourceMissing        = errors.New("object resource key is required")
	ErrContentMissing         = errors.New("content payload is required")

	encoding          = base64.StdEncoding
	defaultExpireTime = time.Second * 30
)
