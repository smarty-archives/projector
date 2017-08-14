package transform

import (
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/pipeline/messaging"
	"github.com/smartystreets/pipeline/projector"
)

func TestHandlerFixture(t *testing.T) {
	gunit.Run(new(HandlerFixture), t)
}

type HandlerFixture struct {
	*gunit.Fixture

	input       chan messaging.Delivery
	output      chan projector.DocumentMessage
	transformer *FakeTransformer
	handler     *Handler
	firstInput  messaging.Delivery
	secondInput messaging.Delivery
	now         time.Time
}

func (this *HandlerFixture) Setup() {
	this.now = clock.UTCNow()
	this.input = make(chan messaging.Delivery, 2)
	this.output = make(chan projector.DocumentMessage, 2)
	this.transformer = NewFakeTransformer()
	this.handler = NewHandler(this.input, this.output, this.transformer)
	this.handler.clock = clock.Freeze(this.now)

	this.firstInput = messaging.Delivery{
		Message: 1,
		Receipt: &FakeAcknowledgement{},
	}
	this.secondInput = messaging.Delivery{
		Message: 2,
		Receipt: &FakeAcknowledgement{},
	}
}

/////////////////////////////////////////////////////////////////

func (this *HandlerFixture) TestTransformerInvokedForEveryInputMessage() {
	this.input <- this.firstInput
	this.input <- this.secondInput
	close(this.input)

	this.handler.Listen()

	this.So(this.transformer.received, should.Resemble, map[interface{}]time.Time{
		this.firstInput.Message:  this.now,
		this.secondInput.Message: this.now,
	})
	this.So(<-this.output, should.Resemble, projector.DocumentMessage{
		Receipt:   this.secondInput.Receipt,
		Documents: collectedDocuments,
	})

	for range this.output {
		// will block unless output channel is closed
	}
}

/////////////////////////////////////////////////////////////////

type FakeTransformer struct {
	received map[interface{}]time.Time
}

func NewFakeTransformer() *FakeTransformer {
	return &FakeTransformer{
		received: make(map[interface{}]time.Time),
	}
}

func (this *FakeTransformer) TransformAllDocuments(message interface{}, now time.Time) {
	this.received[message] = now
}

var collectedDocuments = []projector.Document{
	&fakeDocument{path: "a"},
	&fakeDocument{path: "b"},
	&fakeDocument{path: "c"},
}

func (this *FakeTransformer) Collect() []projector.Document {
	return collectedDocuments
}

/////////////////////////////////////////////////////////////////

type FakeAcknowledgement struct{}

func (this *FakeAcknowledgement) Acknowledge() {}

/////////////////////////////////////////////////////////////////

type fakeDocument struct{ path string }

func (this *fakeDocument) Path() string                                  { return this.path }
func (this *fakeDocument) Lapse(now time.Time) (next projector.Document) { return this }
func (this *fakeDocument) Apply(message interface{}) bool                { return false }

/////////////////////////////////////////////////////////////////
