package gcspersist

import (
	"context"

	"github.com/smartystreets/gcs"
	"github.com/smartystreets/projector/persist"
)

type StorageSettings struct {
	HTTPClient  persist.HTTPClient
	BucketName  string
	PathPrefix  string
	Context     context.Context
	Credentials gcs.Credentials
}
