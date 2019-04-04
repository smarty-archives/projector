package gcspersist

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist/gcs"
)

type Reader struct {
	settings func() StorageSettings
	clock    *clock.Clock
	logger   *logging.Logger
}

func NewReader(settings func() StorageSettings) *Reader {
	return &Reader{settings: settings}
}
func (this *Reader) Read(document projector.Document) error {
	settings := this.settings()
	request, err := this.buildRequest(document.Path(), settings)
	if err != nil {
		return fmt.Errorf("could not create signed request: %s\n", err)
	}

	response, err := settings.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("http client error: '%s'", err.Error())
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode == http.StatusNotFound {
		this.logger.Printf("[INFO] Document not found at '%s'\n", document.Path())
		return nil
	}

	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(document); err != nil {
		return fmt.Errorf("document read error: %s", err.Error())
	}

	document.SetVersion(response.Header.Get("ETag"))
	return nil

}
func (this *Reader) buildRequest(documentPath string, settings StorageSettings) (*http.Request, error) {
	return gcs.NewRequest(gcs.GET,
		gcs.WithCredentials(settings.Credentials),
		gcs.WithBucket(settings.BucketName),
		gcs.WithResource(path.Join("/", settings.PathPrefix, documentPath)),
		gcs.WithExpiration(this.clock.UTCNow().Add(time.Hour*24)))
}
func (this *Reader) ReadPanic(document projector.Document) {
	if err := this.Read(document); err != nil {
		this.logger.Panic(err)
	}
}
