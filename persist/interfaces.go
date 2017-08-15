package persist

import (
	"net/http"

	"github.com/smartystreets/projector"
)

type Writer interface {
	Write(projector.Document)
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}
