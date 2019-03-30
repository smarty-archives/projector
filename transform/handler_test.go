package transform

import (
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging"
	"github.com/smartystreets/projector"
)

func TestHandlerFixture(t *testing.T) {
	gunit.Run(new(HandlerFixture), t)
}

type HandlerFixture struct {
	*gunit.Fixture

	input       chan messaging.Delivery
	output      chan interface{}
	transformer *FakeTransformer
	handler     *Handler
	firstInput  messaging.Delivery
	secondInput messaging.Delivery
	thirdInput  messaging.Delivery
	fourthInput messaging.Delivery
	now         time.Time
}

func (this *HandlerFixture) Setup() {
	this.now = clock.UTCNow()
	this.input = make(chan messaging.Delivery, 2)
	this.output = make(chan interface{}, 2)
	this.transformer = NewFakeTransformer()
	this.handler = NewHandler(this.input, this.output, this.transformer)
	this.handler.clock = clock.Freeze(this.now)

	this.firstInput = messaging.Delivery{
		Message: 1,
		Receipt: 11,
	}
	this.secondInput = messaging.Delivery{
		Message: 2,
		Receipt: 12,
	}
	this.thirdInput = messaging.Delivery{
		Message: 3,
		Receipt: 13,
	}
	this.fourthInput = messaging.Delivery{
		Message: 4,
		Receipt: 14,
	}
}

// ///////////////////////////////////////////////////////////////

func (this *HandlerFixture) TestTransformerInvokedForEveryInputMessage() {
	this.input <- this.firstInput
	this.input <- this.secondInput
	close(this.input)

	this.handler.Listen()

	this.So(this.transformer.received, should.Resemble, map[interface{}]time.Time{
		this.firstInput.Message:  this.now,
		this.secondInput.Message: this.now,
	})
	this.So(<-this.output, should.Equal, this.secondInput.Receipt)

	<-this.output
}

func (this *HandlerFixture) TestTransformerInvokedForMultipleSetsOfInputMessages() {
	go this.handler.Listen()

	this.input <- this.firstInput
	this.input <- this.secondInput
	time.Sleep(time.Millisecond)
	this.input <- this.thirdInput
	this.input <- this.fourthInput
	close(this.input)

	this.So(this.transformer.received[this.firstInput.Message], should.Equal, this.now)
	this.So(this.transformer.received[this.secondInput.Message], should.Equal, this.now)
	this.So(this.transformer.received[this.thirdInput.Message], should.Equal, this.now)
	this.So(this.transformer.received[this.fourthInput.Message], should.Equal, this.now)

	this.So(<-this.output, should.Resemble, this.secondInput.Receipt)
	this.So(<-this.output, should.Resemble, this.fourthInput.Receipt)

	<-this.output
}

// ///////////////////////////////////////////////////////////////

type FakeTransformer struct {
	received map[interface{}]time.Time
}

func NewFakeTransformer() *FakeTransformer {
	return &FakeTransformer{
		received: make(map[interface{}]time.Time),
	}
}

func (this *FakeTransformer) TransformAllDocuments(now time.Time, messages ...interface{}) {
	for _, message := range messages {
		this.received[message] = now
	}
}

// ///////////////////////////////////////////////////////////////

type fakeDocument struct{ path string }

func (this *fakeDocument) Path() string                                  { return this.path }
func (this *fakeDocument) Lapse(now time.Time) (next projector.Document) { return this }
func (this *fakeDocument) Reset()                                        {}
func (this *fakeDocument) Apply(message interface{}) bool                { return false }
func (this *fakeDocument) SetVersion(interface{})                        {}
func (this *fakeDocument) Version() interface{}                          { return "etag" }

// ///////////////////////////////////////////////////////////////
