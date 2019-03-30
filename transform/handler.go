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
	var messages []interface{}

	for delivery := range this.input {
		messages = append(messages, delivery.Message)
		metrics.Measure(transformQueueDepth, int64(len(this.input)))

		if len(this.input) > 0 {
			continue
		}

		now := this.clock.UTCNow()
		this.transformer.TransformAllDocuments(now, messages...)
		messages = messages[0:0]

		this.output <- projector.DocumentMessage{
			Receipt:   delivery.Receipt,
			Documents: this.transformer.Collect(),
		}
	}

	close(this.output)
}

var transformQueueDepth = metrics.AddGauge("pipeline:transform-phase-backlog-depth", time.Second*30)
