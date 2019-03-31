package s3persist

import (
	"net/url"

	"github.com/smartystreets/projector/persist"
)

type Storage struct {
	*Reader
	*Writer
}

func NewStorage(address *url.URL, accessKey, secretKey string, client persist.HTTPClient) *Storage {
	return &Storage{
		Reader: NewReader(address, accessKey, secretKey, client),
		Writer: NewWriter(address, accessKey, secretKey, client),
	}
}
