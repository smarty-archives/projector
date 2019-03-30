package persist

import (
	"net/http"

	"github.com/smartystreets/projector"
)

type Reader interface {
	Read(path string, document interface{}) error
	ReadPanic(path string, document interface{})
}

type Writer interface {
	Write(projector.Document)
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}
