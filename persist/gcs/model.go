package gcs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

type model struct {
	credentials      Credentials
	method           string
	bucket           string
	resource         string
	contentMD5       string
	contentType      string
	contentEncoding  string
	generation       string
	etag             string
	context          context.Context
	encryption       ServerSideEncryption
	hasContentLength bool
	contentLength    int64
	content          io.ReadSeeker

	// computed fields
	objectKey string
	epoch     string
}

func newModel(method string, options []Option) model {
	this := &model{method: method, context: context.Background()}
	WithExpiration(defaultExpiration())(this)

	this.applyOptions(options)
	this.objectKey = path.Join("/", this.bucket, this.resource)

	return *this
}

func (this *model) applyOptions(options []Option) {
	for _, option := range options {
		if option != nil {
			option(this)
		}
	}
}
func (this model) validate() error {
	if len(this.method) == 0 {
		return ErrMissingHTTPMethod
	} else if this.method != GET && this.method != PUT {
		return ErrUnrecognizedHTTPMethod
	} else if len(this.bucket) == 0 {
		return ErrBucketMissing
	} else if len(this.resource) == 0 {
		return ErrResourceMissing
	} else if this.method == PUT && this.content == nil {
		return ErrContentMissing
	}
	return nil
}

func (this model) buildRequest() (*http.Request, error) {
	if signature, err := this.calculateSignature(); err != nil {
		return nil, err
	} else if request, err := http.NewRequest(this.method, this.buildSignedURL(signature), this.content); err != nil {
		return nil, err
	} else {
		this.appendHeaders(request)
		return request.WithContext(this.context), nil
	}
}
func (this model) calculateSignature() (string, error) {
	buffer := bytes.NewBuffer(nil)
	this.appendToBuffer(buffer)

	if signed, err := this.credentials.PrivateKey.Sign(buffer.Bytes()); err != nil {
		return "", err
	} else {
		return base64.StdEncoding.EncodeToString(signed), nil
	}
}
func (this model) appendToBuffer(buffer io.Writer) {
	_, _ = fmt.Fprintf(buffer, "%s\n%s\n%s\n%s\n", this.method, this.contentMD5, this.contentType, this.epoch)

	if this.encryption != ServerSideEncryptionNone && this.method == PUT {
		_, _ = fmt.Fprintf(buffer, "x-goog-encryption-algorithm:%s\n", this.generation)
	}

	if len(this.generation) > 0 && this.method == PUT {
		_, _ = fmt.Fprintf(buffer, "x-goog-if-generation-match:%s\n", this.generation)
	}

	_, _ = fmt.Fprintf(buffer, "%s", this.objectKey)
}

func (this model) buildSignedURL(signature string) string {
	targetURL := &url.URL{Scheme: "https", Host: "storage.googleapis.com", Path: this.objectKey}
	query := targetURL.Query()
	query.Set("GoogleAccessId", this.credentials.AccessID)
	query.Set("Expires", this.epoch)
	query.Set("Signature", signature)
	targetURL.RawQuery = query.Encode()
	return targetURL.String()
}
func (this model) appendHeaders(request *http.Request) {
	if this.hasContentLength {
		request.ContentLength = this.contentLength
	}

	headers := request.Header
	if len(this.contentType) > 0 {
		headers.Set("Content-Type", this.contentType)
	}

	if len(this.contentMD5) > 0 {
		headers.Set("Content-MD5", this.contentMD5)
	}

	if len(this.contentEncoding) > 0 {
		headers.Set("Content-Encoding", this.contentEncoding)
	}

	if len(this.generation) > 0 && this.method == PUT {
		headers.Set("x-goog-if-generation-match", this.generation)
	} else if len(this.etag) > 0 && this.method == GET {
		headers.Set("If-None-Match", this.etag)
	}

	if this.encryption != ServerSideEncryptionNone {
		headers.Set("x-goog-if-generation-match", this.encryption.String())
	}
}

func defaultExpiration() time.Time { return time.Now().UTC().Add(defaultExpireTime) }
