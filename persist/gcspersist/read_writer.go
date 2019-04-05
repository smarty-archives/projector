package gcspersist

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/gcs"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type ReadWriter struct {
	settings func() StorageSettings
	clock    *clock.Clock
	logger   *logging.Logger
}

func NewReadWriter(settings func() StorageSettings) *ReadWriter {
	return &ReadWriter{settings: settings}
}

func (this *ReadWriter) Name() string { return "Google Cloud Storage" }

func (this *ReadWriter) ReadPanic(document projector.Document) {
	if err := this.Read(document); err != nil {
		this.logger.Panic(err)
	}
}
func (this *ReadWriter) Read(document projector.Document) error {
	settings := this.settings()
	resource := path.Join("/", settings.PathPrefix, document.Path())
	expiration := this.clock.UTCNow().Add(time.Hour * 24)

	return this.execute(document, settings.HTTPClient, gcs.GET,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(resource),
		gcs.WithExpiration(expiration))
}
func (this *ReadWriter) Write(document projector.Document) error {
	settings := this.settings()
	resource := path.Join("/", settings.PathPrefix, document.Path())
	expiration := this.clock.UTCNow().Add(time.Hour * 24)
	generation, _ := document.Version().(string)
	body := this.serialize(document)
	checksum := md5.Sum(body)

	return this.execute(document, settings.HTTPClient, gcs.PUT,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(resource),
		gcs.WithExpiration(expiration),
		gcs.PutWithGeneration(generation),
		gcs.PutWithContentBytes(body),
		gcs.PutWithContentEncoding("gzip"),
		gcs.PutWithContentType("application/json"),
		gcs.PutWithContentMD5(checksum[:]))
}

func (this *ReadWriter) serialize(document projector.Document) []byte {
	buffer := bytes.NewBuffer([]byte{})
	writer, _ := gzip.NewWriterLevel(buffer, gzip.BestCompression)

	if err := json.NewEncoder(writer).Encode(document); err != nil {
		this.logger.Panic(err)
		return nil
	}

	_ = writer.Close() // flush the buffer too
	return buffer.Bytes()

}
func (this *ReadWriter) deserialize(document projector.Document, response *http.Response) error {
	if response.ContentLength == 0 {
		return nil
	}

	err := json.NewDecoder(response.Body).Decode(document)
	if err != nil {
		return fmt.Errorf("document read error: '%s'", err.Error())
	}

	return nil
}

func (this *ReadWriter) execute(document projector.Document, client persist.HTTPClient, method string, options ...gcs.Option) error {
	request, err := gcs.NewRequest(method, options...)
	if err != nil {
		return fmt.Errorf("could not create signed request: %s\n", err)
	}

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("http client error: '%s'", err)
	}

	generation, err := this.handleResponse(document, response)
	if err != nil {
		return err
	}

	document.SetVersion(generation)
	return nil
}
func (this *ReadWriter) handleResponse(document projector.Document, response *http.Response) (string, error) {
	defer func() { _ = response.Body.Close() }()
	switch response.StatusCode {
	case http.StatusOK:
		return response.Header.Get("x-goog-generation"), this.deserialize(document, response)
	case http.StatusNotFound:
		this.logger.Printf("[INFO] Document not found at '%s'\n", document.Path())
		return "", nil
	case http.StatusPreconditionFailed:
		this.logger.Printf("[INFO] Document on remote storage has changed '%s'\n", document.Path())
		return "", persist.ErrConcurrentWrite
	default:
		return "", fmt.Errorf("non-200 http status code: %s", response.Status)
	}
}
