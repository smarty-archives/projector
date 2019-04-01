package transform

import (
	"github.com/smartystreets/listeners"
	"github.com/smartystreets/messaging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

func NewHandler(i <-chan messaging.Delivery, o chan<- interface{}, d []projector.Document, rw persist.ReadWriter) listeners.Listener {
	return newHandler(i, o, newTransformer(d, rw))
}

func newTransformer(docs []projector.Document, rw persist.ReadWriter) Transformer {
	var transformers []Transformer

	for _, document := range docs {
		if document != nil {
			transformers = append(transformers, newSimpleTransformer(document, rw))
		}
	}

	return newMultiTransformer(transformers)
}
