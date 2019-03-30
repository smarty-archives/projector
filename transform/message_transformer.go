package transform

import (
	"time"

	"github.com/smartystreets/projector"
)

type MessageTransformer struct {
	documents []projector.Document
	changed   map[projector.Document]struct{}
}

func NewMessageTransformer(documents []projector.Document) *MessageTransformer {
	return &MessageTransformer{
		documents: documents,
		changed:   make(map[projector.Document]struct{}, 16),
	}
}

func (this *MessageTransformer) TransformAllDocuments(now time.Time, messages ...interface{}) {
	for _, message := range messages {
		this.transformAllDocuments(now, message)
	}
}
func (this *MessageTransformer) transformAllDocuments(now time.Time, message interface{}) {
	if message == nil {
		return
	}

	for i, doc := range this.documents {
		doc = doc.Lapse(now)
		this.documents[i] = doc
		if doc.Apply(message) {
			this.changed[doc] = struct{}{}
		}
	}
}
