package transform

import (
	"io"
	"time"

	"github.com/smartystreets/projector"
)

type Transformer interface {
	TransformAllDocuments(message interface{}, now time.Time)
	Collect() []projector.Document
}

type Cloner interface {
	Clone(projector.Document) projector.Document
}

type ResetReadWriter interface {
	Reset() // as in bytes.Buffer.Reset()
	io.ReadWriter
}
