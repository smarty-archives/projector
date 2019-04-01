package transform

import (
	"sync"
	"time"

	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type Transformer interface {
	Transform(time.Time, []interface{})
}

type multiTransformer struct {
	transformers []*simpleTransformer
	waiter       sync.WaitGroup
}

func newTransformer(store persist.ReadWriter, documents ...projector.Document) Transformer {
	var transformers []*simpleTransformer
	for _, document := range documents {
		transformers = append(transformers, newSimpleTransformer(document, store))
	}

	return &multiTransformer{transformers: transformers}
}
func (this *multiTransformer) Transform(now time.Time, messages []interface{}) {
	count := len(this.transformers)
	this.waiter.Add(count)

	for i := 0; i < count; i++ {
		go this.transform(i, now, messages) // this for loop is safe to execute because it evaluates "i" before "go"
	}

	this.waiter.Wait()
}
func (this *multiTransformer) transform(index int, now time.Time, messages []interface{}) {
	this.transformers[index].Transform(now, messages)
	this.waiter.Done()
}

/* ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

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
