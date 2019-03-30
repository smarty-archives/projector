package persist

import (
	"sync"

	"github.com/smartystreets/projector"
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

	for _, document := range this.pending {
		go this.persist(document)
	}

	this.waiter.Wait()

}
func (this *Handler) persist(document projector.Document) {
	_, _ = this.writer.Write(document)
	this.waiter.Done()
}

func (this *Handler) sendLatestAcknowledgement(receipt interface{}) {
	this.output <- receipt
}

func (this *Handler) prepareForNextBatch() {
	this.pending = make(map[string]projector.Document)
}
