package persist

import (
	"sync"
	"time"

	"github.com/smartystreets/clock"
	"github.com/smartystreets/metrics"
	"github.com/smartystreets/pipeline/projector"
)

type Handler struct {
	input  chan projector.DocumentMessage
	output chan<- interface{}
	writer Writer

	pending map[string]projector.Document
	waiter  *sync.WaitGroup
}

func NewHandler(input chan projector.DocumentMessage, output chan<- interface{}, writer Writer) *Handler {
	return &Handler{
		input:   input,
		output:  output,
		writer:  writer,
		pending: make(map[string]projector.Document),
		waiter:  new(sync.WaitGroup),
	}
}

func (this *Handler) Listen() {
	for message := range this.input {
		metrics.Measure(DepthPersistQueue, int64(len(this.input)))

		this.addToBatch(message)

		if len(this.input) == 0 {
			this.handleCurrentBatch(message.Receipt)
		}
	}

	close(this.output)
}

func (this *Handler) addToBatch(message projector.DocumentMessage) {
	for _, document := range message.Documents {
		this.pending[document.Path()] = document
	}
}

func (this *Handler) handleCurrentBatch(receipt interface{}) {
	this.persistPendingDocuments()
	this.sendLatestAcknowledgement(receipt)
	this.prepareForNextBatch()
}

func (this *Handler) persistPendingDocuments() {
	this.waiter.Add(len(this.pending))
	metrics.Measure(DocumentsToSave, int64(len(this.pending)))

	for _, document := range this.pending {
		go this.persist(document)
	}

	this.waiter.Wait()
}
func (this *Handler) persist(document projector.Document) {
	started := clock.UTCNow()
	this.writer.Write(document)
	metrics.Measure(DocumentWriteLatency, milliseconds(time.Since(started)))
	this.waiter.Done()
}

func milliseconds(duration time.Duration) int64 { return microseconds(duration) / 1000 }
func microseconds(duration time.Duration) int64 { return int64(duration.Nanoseconds() / 1000) }

func (this *Handler) sendLatestAcknowledgement(receipt interface{}) {
	this.output <- receipt
}

func (this *Handler) prepareForNextBatch() {
	this.pending = make(map[string]projector.Document)
}

var (
	DepthPersistQueue    = metrics.AddGauge("pipeline:persist-phase-backlog-depth", time.Second*300)
	DocumentsToSave      = metrics.AddGauge("pipeline:documents-to-save", time.Second*300)
	DocumentWriteLatency = metrics.AddGauge("pipeline:document-write-latency-milliseconds", time.Second*300)
)
