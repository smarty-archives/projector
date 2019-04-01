package transform

import (
	"time"

	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type SimpleTransformer struct {
	document projector.Document
	storage  persist.ReadWriter
}

func newSimpleTransformer(document projector.Document, storage persist.ReadWriter) *SimpleTransformer {
	return &SimpleTransformer{document: document, storage: storage}
}

func (this *SimpleTransformer) Transform(now time.Time, messages []interface{}) {
	this.document = this.document.Lapse(now)
	for this.apply(messages) && !this.save() {
	}
}

func (this *SimpleTransformer) apply(messages []interface{}) (modified bool) {
	for _, message := range messages {
		if message != nil {
			modified = this.document.Apply(message) || modified
		}
	}
	return modified
}

func (this *SimpleTransformer) save() bool {
	if _, err := this.storage.Write(this.document); err == nil {
		return true
	} else {
		this.document.Reset()
		_, _ = this.storage.Read(this.document.Path(), this.document)
		return false
	}
}
