package gcspersist

import (
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
	"github.com/smartystreets/projector/persist/gcs"
)

type Reader struct {
	settings func() StorageSettings
	clock    *clock.Clock
}

type StorageSettings struct {
	HTTPClient  persist.HTTPClient
	BucketName  string
	PathPrefix  string
	Credentials gcs.Credentials
}

func NewReader(settings func() StorageSettings) *Reader {
	return &Reader{settings: settings}
}

func (this *Reader) Read(document projector.Document) error {
	settings := this.settings()
	if request, err := this.buildRequest(document.Path(), settings); err != nil {
		return err
	} else {
		fmt.Println(request.URL.String())
		return nil
	}
}
func (this *Reader) buildRequest(documentPath string, settings StorageSettings) (*http.Request, error) {
	expiration := this.clock.UTCNow().Add(time.Hour * 24)
	documentPath = path.Join(settings.PathPrefix, documentPath)

	if request, err := gcs.NewRequest("GET", settings.BucketName, documentPath, expiration, settings.Credentials); err != nil {
		return nil, err
	} else if signedURL, err := request.SignedURL(); err != nil {
		return nil, err
	} else {
		return http.NewRequest("GET", signedURL, nil)
	}
}

func (this *Reader) expiration() string {
	expires := this.clock.UTCNow().Add(time.Hour * 24)
	return fmt.Sprintf("%d", expires.Unix())
}

func (this *Reader) ReadPanic(document projector.Document) {
	panic("implement me")
}
