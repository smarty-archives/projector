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

	return this.execute(document, settings.HTTPClient, gcs.GET,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(path.Join("/", settings.PathPrefix, document.Path())),
		gcs.WithExpiration(this.clock.UTCNow().Add(time.Hour*24)))
}
func (this *ReadWriter) Write(document projector.Document) error {
	settings := this.settings()
	body := this.serialize(document)
	checksum := md5.Sum(body)
	generation, _ := document.Version().(string)

	return this.execute(document, settings.HTTPClient, gcs.PUT,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(path.Join("/", settings.PathPrefix, document.Path())),
		gcs.WithExpiration(this.clock.UTCNow().Add(time.Hour*24)),
		gcs.PutWithContentBytes(body),
		gcs.PutWithContentEncoding("gzip"),
		gcs.PutWithContentType("application/json"),
		gcs.PutWithContentMD5(checksum[:]),
		gcs.PutWithServerSideEncryption(),
		gcs.PutWithGeneration(generation))
}

func (this *ReadWriter) serialize(document projector.Document) []byte {
	buffer := bytes.NewBuffer([]byte{})
	writer, _ := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	defer func() { _ = writer.Close() }()

	if err := json.NewEncoder(writer).Encode(document); err != nil {
		this.logger.Panic(err)
	} else {
		return buffer.Bytes()
	}
}
func (this *ReadWriter) deserialize(document projector.Document, response *http.Response) error {
	if response.ContentLength == 0 {
		return nil
	} else if err := json.NewDecoder(response.Body).Decode(document); err != nil {
		return fmt.Errorf("document read error: '%s'", err.Error())
	} else {
		return nil
	}
}

func (this *ReadWriter) execute(document projector.Document, client persist.HTTPClient, method string, options ...gcs.Option) error {
	if request, err := gcs.NewRequest(method, options...); err != nil {
		return fmt.Errorf("could not create signed request: %s\n", err)
	} else if response, err := client.Do(request); err != nil {
		return fmt.Errorf("http client error: '%s'", err)
	} else if generation, err := this.handleResponse(document, response); err != nil {
		return err
	} else {
		document.SetVersion(generation)
		return nil
	}
}
func (this *ReadWriter) handleResponse(document projector.Document, response *http.Response) (string, error) {
	defer func() { _ = response.Body.Close() }()
	switch response.StatusCode {
	case http.StatusOK:
		return response.Header.Get(headerGeneration), this.deserialize(document, response)
	case http.StatusNotFound:
		return "", nil
	case http.StatusPreconditionFailed:
		return "", persist.ErrConcurrentWrite
	default:
		return "", fmt.Errorf("non-200 http status code: %s", response.Status)
	}
}

const headerGeneration = "x-goog-generation"
