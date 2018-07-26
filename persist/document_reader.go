package persist

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/smartystreets/logging"
	"github.com/smartystreets/s3"
)

type DocumentReader struct {
	logger *logging.Logger

	s3     *s3.S3
	bucket string
	client HTTPClient
}

func NewDocumentReader(bucket string, s3 *s3.S3, client HTTPClient) *DocumentReader {
	return &DocumentReader{
		s3:     s3,
		bucket: bucket,
		client: client,
	}
}

func (this *DocumentReader) Read(path string, document interface{}) error {
	request, err := this.s3.SignedGetRequest(this.bucket, s3.Key(path))
	if err != nil {
		return fmt.Errorf("Could not create signed request: '%s'", err.Error())
	}

	response, err := this.client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP Client Error: '%s'", err.Error())
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		this.logger.Printf("[INFO] Document not found at '%s'\n", path)
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

	return nil
}

func (this *DocumentReader) ReadPanic(path string, document interface{}) {
	if err := this.Read(path, document); err != nil {
		this.logger.Panic(err)
	}
}
