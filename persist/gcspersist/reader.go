package gcspersist

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type Reader struct {
	settings func() StorageSettings
	clock    *clock.Clock
}

type StorageSettings struct {
	HTTPClient  persist.HTTPClient
	BucketName  string
	PathPrefix  string
	Credentials Credentials
}

func NewReader(settings func() StorageSettings) *Reader {
	return &Reader{settings: settings}
}

func (this *Reader) Read(document projector.Document) error {
	settings := this.settings()

	if request, err := this.buildRequest(document.Path(), settings); err != nil {
		return err
	} else {
		log.Println("FULL URL:", request.URL.String())
		return nil
	}
}
func (this *Reader) buildRequest(documentPath string, settings StorageSettings) (*http.Request, error) {
	expiration := this.clock.UTCNow().Add(time.Hour * 24)

	if request, err := NewRequest("GET", documentPath, expiration, settings); err != nil {
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
