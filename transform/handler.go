package transform

import (
	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type Handler struct {
	input       <-chan messaging.Delivery
	output      chan<- interface{}
	transformer Transformer
	clock       *clock.Clock
	messages    []interface{}
}

func NewHandler(input <-chan messaging.Delivery, output chan<- interface{}, transformer Transformer) *Handler {
	return &Handler{input: input, output: output, transformer: transformer}
}

func (this *Handler) Listen() {
	for delivery := range this.input {
		this.messages = append(this.messages, delivery.Message)
		if len(this.input) == 0 {
			this.transform(delivery.Receipt)
		}
	}

	close(this.output)
}
func (this *Handler) transform(receipt interface{}) {
	this.transformer.TransformAllDocuments(this.clock.UTCNow(), this.messages...)
	this.output <- receipt
	this.messages = this.messages[0:0]
}
