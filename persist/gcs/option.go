package gcs

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"time"
)

type Option func(*model)

func WithCredentials(credentials Credentials) Option {
	return func(this *model) { this.credentials = credentials }
}
func WithBucket(value string) Option {
	return func(this *model) { this.bucket = strings.TrimSpace(value) }
}
func WithResource(value string) Option {
	return func(this *model) { this.resource = strings.TrimSpace(value) }
}
func WithExpiration(value time.Time) Option {
	return func(this *model) { this.epoch = strconv.FormatInt(value.Unix(), 10) }
}
func WithContext(value context.Context) Option {
	return func(this *model) { this.context = value }
}

func GetWithETag(value string) Option {
	return func(this *model) { this.etag = strings.TrimSpace(value) }
}
func PutWithGeneration(value int64) Option {
	return func(this *model) { this.generation = strconv.FormatInt(value, 10) }
}

func PutWithServerSideEncryption(value ServerSideEncryption) Option {
	return func(this *model) { this.encryption = value }
}
func PutWithContentString(value string) Option {
	return func(this *model) { PutWithContentBytes([]byte(value))(this) }
}
func PutWithContentBytes(value []byte) Option {
	return func(this *model) {
		this.content = bytes.NewReader(value)
		this.contentLength = int64(len(value))
		this.hasContentLength = true
	}
}
func PutWithContent(value io.ReadSeeker) Option {
	return func(this *model) { this.content = value }
}

func PutWithContentType(value string) Option {
	return func(this *model) { this.contentType = strings.TrimSpace(value) }
}
func PutWithContentLength(value int64) Option {
	return func(this *model) { this.contentLength = value; this.hasContentLength = true }
}
func PutWithContentMD5(value []byte) Option {
	return func(this *model) { this.contentMD5 = encoding.EncodeToString(value) }
}
func PutWithContentEncoding(value string) Option {
	return func(this *model) { this.contentEncoding = value }
}

func Nop(_ *model) {}
func WithCompositeOption(options ...Option) Option {
	return func(this *model) { this.applyOptions(options) }
}
func WithConditionalOption(option Option, condition bool) Option {
	if condition {
		return option
	} else {
		return nil
	}
}
