package gcspersist

import (
	"github.com/smartystreets/projector/persist"
	"github.com/smartystreets/projector/persist/gcs"
)

type StorageSettings struct {
	HTTPClient  persist.HTTPClient
	BucketName  string
	PathPrefix  string
	Credentials gcs.Credentials
}
