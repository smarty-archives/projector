package transform

import (
	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
)

type Handler struct {
	input       <-chan messaging.Delivery
	output      chan<- interface{}
	transformer Transformer
	messages    []interface{}
	clock       *clock.Clock
}

func newHandler(input <-chan messaging.Delivery, output chan<- interface{}, transformer Transformer) *Handler {
	return &Handler{input: input, output: output, transformer: transformer}
}

func (this *Handler) Listen() {
	for delivery := range this.input {
		this.messages = append(this.messages, delivery.Message)
		if len(this.input) > 0 {
			continue
		}

		this.transformer.Transform(this.clock.UTCNow(), this.messages)
		this.output <- delivery.Receipt
		this.messages = this.messages[0:0]
	}

	close(this.output)
}
