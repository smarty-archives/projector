package s3persist

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector/persist"
	"github.com/smartystreets/s3"
)

type Reader struct {
	logger *logging.Logger

	storage     s3.Option
	credentials s3.Option
	client      persist.HTTPClient
}

func NewReader(storageAddress *url.URL, accessKey, secretKey string, client persist.HTTPClient) *Reader {
	return &Reader{
		storage:     s3.StorageAddress(storageAddress),
		credentials: s3.Credentials(accessKey, secretKey),
		client:      client,
	}
}

func (this *Reader) Read(path string, document interface{}) (interface{}, error) {
	request, err := s3.NewRequest(s3.GET, this.credentials, this.storage, s3.Key(path))
	if err != nil {
		return nil, fmt.Errorf("Could not create signed request: '%s'", err.Error())
	}

	response, err := this.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("HTTP Client Error: '%s'", err.Error())
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound {
		this.logger.Printf("[INFO] Document not found at '%s'\n", path)
		return nil, nil
	}

	reader := response.Body.(io.Reader)
	if response.Header.Get("Content-Encoding") == "gzip" {
		reader, _ = gzip.NewReader(reader)
	}

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(document); err != nil {
		return nil, fmt.Errorf("Document read error: '%s'", err.Error())
	}

	return response.Header.Get("ETag"), nil
}

func (this *Reader) ReadPanic(path string, document interface{}) interface{} {
	if etag, err := this.Read(path, document); err != nil {
		this.logger.Panic(err)
		return nil
	} else {
		return etag
	}
}
