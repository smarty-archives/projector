package s3persist

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
	"github.com/smartystreets/s3"
)

type Reader struct {
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

func (this *Reader) Read(document projector.Document) error {
	request, err := s3.NewRequest(s3.GET, this.credentials, this.storage, s3.Key(document.Path()))
	if err != nil {
		return fmt.Errorf("Could not create signed request: '%s'", err.Error())
	}

	response, err := this.client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP Client Error: '%s'", err.Error())
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound {
		log.Printf("[INFO] Document not found at '%s'\n", document.Path())
		return nil
	}

	reader := response.Body.(io.Reader)
	if response.Header.Get("Content-Encoding") == "gzip" {
		reader, _ = gzip.NewReader(reader)
	}

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(document); err != nil {
		return fmt.Errorf("Document read error: '%s'", err.Error())
	}

	document.SetVersion(response.Header.Get("ETag"))
	return nil
}

func (this *Reader) ReadPanic(document projector.Document) {
	if err := this.Read(document); err != nil {
		log.Panic(err)
	}
}
