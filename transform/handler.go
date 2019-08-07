package transform

import (
	"github.com/smartystreets/clock"
	"github.com/smartystreets/listeners"
	"github.com/smartystreets/messaging/v2"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

type Handler struct {
	input       <-chan messaging.Delivery
	output      chan<- interface{}
	transformer Transformer
	messages    []interface{}
	clock       *clock.Clock
}

func NewHandler(i <-chan messaging.Delivery, o chan<- interface{}, rw persist.ReadWriter, d ...projector.Document) listeners.Listener {
	return newHandler(i, o, newTransformer(rw, d...))
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
