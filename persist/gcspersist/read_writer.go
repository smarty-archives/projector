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

	request, err := gcs.NewRequest(gcs.GET,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(path.Join("/", settings.PathPrefix, document.Path())),
		gcs.WithExpiration(this.clock.UTCNow().Add(time.Hour*24)))

	if err != nil {
		return fmt.Errorf("could not create signed request: %s\n", err)
	}

	response, err := settings.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("http client error: '%s'", err)
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound {
		this.logger.Printf("[INFO] Document not found at '%s'\n", document.Path())
		return nil
	}

	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(document); err != nil {
		return fmt.Errorf("document read error: %s", err)
	}

	document.SetVersion(response.Header.Get(headerGeneration))
	return nil

}

func (this *ReadWriter) Write(document projector.Document) error {
	settings := this.settings()
	body := this.serialize(document)
	checksum := md5.Sum(body)
	generation, _ := document.Version().(string)

	request, err := gcs.NewRequest(gcs.PUT,
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

	if err != nil {
		this.logger.Panic(err)
		return nil
	} else if response, err := settings.HTTPClient.Do(request); err != nil {
		this.logger.Panic(err)
		return nil
	} else if generation, err := this.handleResponse(response); err == persist.ErrConcurrentWrite {
		return err
	} else if err != nil {
		this.logger.Panic(err)
		return nil
	} else {
		document.SetVersion(generation)
		return nil
	}
}
func (this *ReadWriter) serialize(document projector.Document) []byte {
	buffer := bytes.NewBuffer([]byte{})
	gzipWriter, _ := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	encoder := json.NewEncoder(gzipWriter)

	if err := encoder.Encode(document); err != nil {
		this.logger.Panic(err)
	}

	_ = gzipWriter.Close()
	return buffer.Bytes()
}
func (this *ReadWriter) handleResponse(response *http.Response) (string, error) {
	defer func() { _ = response.Body.Close() }()

	switch response.StatusCode {
	case http.StatusOK:
		return response.Header.Get(headerGeneration), nil
	case http.StatusPreconditionFailed:
		return "", persist.ErrConcurrentWrite
	default:
		return "", fmt.Errorf("non-200 http status code: %s", response.Status)
	}
}

const headerGeneration = "x-goog-generation"
