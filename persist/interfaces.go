package persist

import (
	"errors"
	"net/http"

	"github.com/smartystreets/projector"
)

type Reader interface {
	Read(path string, document interface{}) (interface{}, error)
	ReadPanic(path string, document interface{}) interface{}
}

// Writer writes the document and gives back the updated generation/etag/ID of the document with storage.
// Even in the case of an error, the ID will be returned
type Writer interface {
	Write(projector.Document) (interface{}, error)
}

type ReadWriter interface {
	Reader
	Writer
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

var ErrConcurrentWrite = errors.New("the document has been updated by another process")
