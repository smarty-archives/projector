package transform

import "time"

type Transformer interface {
	TransformAllDocuments(now time.Time, messages ...interface{})
}
