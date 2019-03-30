package transform

import (
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/messaging"
	"github.com/smartystreets/metrics"
	"github.com/smartystreets/projector"
)

type Handler struct {
	input       <-chan messaging.Delivery
	output      chan<- projector.DocumentMessage
	transformer Transformer
	clock       *clock.Clock
}

func NewHandler(input <-chan messaging.Delivery, output chan<- projector.DocumentMessage, transformer Transformer) *Handler {
	return &Handler{input: input, output: output, transformer: transformer}
}

func (this *Handler) Listen() {
	for message := range this.input {
		now := this.clock.UTCNow()

		metrics.Measure(transformQueueDepth, int64(len(this.input)))

		this.transformer.TransformAllDocuments(message.Message, now)

		// NOTE: this code only appears to be run in "projectors" which means that
		// it's safe to call persist right here in this phase
		// but we first want to collect all possible messages from the input queue until it's empty
		// or we reach an arbitrary number

		// TODO: collect all messages, then pass them off to transformer
		// then collect and persist those that changed
		// if failure to persist, evict those that failed and re-apply messages for just those that failed
		// don't do that in a separate go-routine

		if len(this.input) == 0 {
			this.output <- projector.DocumentMessage{
				Receipt:   message.Receipt,
				Documents: this.transformer.Collect(),
			}
		}
	}

	close(this.output)
}

var transformQueueDepth = metrics.AddGauge("pipeline:transform-phase-backlog-depth", time.Second*30)
