package transform

import (
	"time"

	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type simpleTransformer struct {
	document projector.Document
	storage  persist.ReadWriter
}

func newSimpleTransformer(document projector.Document, storage persist.ReadWriter) *simpleTransformer {
	return &simpleTransformer{document: document, storage: storage}
}

func (this *simpleTransformer) Transform(now time.Time, messages []interface{}) {
	this.document = this.document.Lapse(now)
	for this.apply(messages) && !this.save() {
	}
}

func (this *simpleTransformer) apply(messages []interface{}) (modified bool) {
	for _, message := range messages {
		if message != nil {
			modified = this.document.Apply(message) || modified
		}
	}
	return modified
}

func (this *simpleTransformer) save() bool {
	if err := this.storage.Write(this.document); err == nil {
		return true
	} else {
		this.document.Reset()
		_ = this.storage.Read(this.document.Path(), this.document)
		return false
	}
}
