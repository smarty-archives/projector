package transform

import (
	"io"
	"time"

	"github.com/smartystreets/projector"
)

type Transformer interface {
	TransformAllDocuments(now time.Time, messages ...interface{})
	Collect() []projector.Document
}

type Cloner interface {
	Clone(projector.Document) projector.Document
}

type ResetReadWriter interface {
	Reset() // as in bytes.Buffer.Reset()
	io.ReadWriter
}
