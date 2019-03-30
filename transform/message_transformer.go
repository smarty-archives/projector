package transform

import (
	"sync"
	"time"

	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type MessageTransformer struct {
	documents []projector.Document
	waiter    sync.WaitGroup
	storage   persist.ReadWriter
}

func NewMessageTransformer(documents []projector.Document, storage persist.ReadWriter) *MessageTransformer {
	return &MessageTransformer{documents: documents, storage: storage}
}

func (this *MessageTransformer) TransformAllDocuments(now time.Time, messages ...interface{}) {
	this.waiter.Add(len(this.documents))
	this.transformAllDocuments(now, messages)
	this.waiter.Wait()
}
func (this *MessageTransformer) transformAllDocuments(now time.Time, messages []interface{}) {
	for index := range this.documents {
		go this.applyAndSave(index, now, messages)
	}
}

func (this *MessageTransformer) applyAndSave(index int, now time.Time, messages []interface{}) {
	defer this.waiter.Done()
	this.documents[index] = this.documents[index].Lapse(now)
	for {
		if !this.apply(index, now, messages) {
			break // messages don't apply, we're done
		} else if this.save(index) {
			break // the document didn't save, re-apply the messages
		}
	}
}
func (this *MessageTransformer) apply(index int, now time.Time, messages []interface{}) (modified bool) {
	for _, message := range messages {
		if message != nil {
			modified = this.documents[index].Apply(message) || modified
		}
	}
	return modified
}
func (this *MessageTransformer) save(index int) bool {
	document := this.documents[index]
	if etag, err := this.storage.Write(document); err == nil {
		document.SetVersion(etag)
		return true
	} else {
		path := document.Path()
		document.Reset()
		etag, _ = this.storage.Read(path, document)
		document.SetVersion(etag)
		return false
	}
}
