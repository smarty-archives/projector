package transform

import (
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging"
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
	inputs      []messaging.Delivery
	now         time.Time
}

func (this *HandlerFixture) Setup() {
	this.now = clock.UTCNow()
	this.input = make(chan messaging.Delivery, 4)
	this.output = make(chan interface{}, 2)
	this.transformer = NewFakeTransformer()
	this.handler = NewHandler(this.input, this.output, this.transformer)
	this.handler.clock = clock.Freeze(this.now)

	this.inputs = []messaging.Delivery{
		{Message: 1, Receipt: 11},
		{Message: 2, Receipt: 12},
		{Message: 3, Receipt: 13},
		{Message: 4, Receipt: 14},
	}
}

func (this *HandlerFixture) TestAllDeliveriesHandled() {
	go this.handler.Listen()
	this.input <- this.inputs[0]
	this.input <- this.inputs[1]
	time.Sleep(time.Millisecond*10)
	this.input <- this.inputs[2]
	this.input <- this.inputs[3]
	close(this.input)
	time.Sleep(time.Millisecond*10)

	this.So(this.transformer.received, should.HaveLength, len(this.inputs))
	for _, delivery := range this.inputs {
		this.So(this.transformer.received[delivery.Message], should.Equal, this.now)
	}
	this.So(this.transformer.received[this.inputs[0].Message], should.Resemble, this.now)
	this.So(<-this.output, should.Equal, this.inputs[1].Receipt)
	this.So(<-this.output, should.Equal, this.inputs[3].Receipt)
	<-this.output
}

// ///////////////////////////////////////////////////////////////

type FakeTransformer struct {
	received map[interface{}]time.Time
}

func NewFakeTransformer() *FakeTransformer {
	return &FakeTransformer{received: make(map[interface{}]time.Time)}
}
func (this *FakeTransformer) TransformAllDocuments(now time.Time, messages ...interface{}) {
	for _, message := range messages {
		this.received[message] = now
	}
}
