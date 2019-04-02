package s3persist

import (
	"net/url"

	"github.com/smartystreets/projector/persist"
)

type ReadWriter struct {
	*Reader
	*Writer
}

func NewStorage(address *url.URL, accessKey, secretKey string, client persist.HTTPClient) persist.ReadWriter {
	return &ReadWriter{
		Reader: NewReader(address, accessKey, secretKey, client),
		Writer: NewWriter(address, accessKey, secretKey, client),
	}
}

func (this *ReadWriter) Name() string { return "AWS S3" }
