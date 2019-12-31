package transform

import (
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/messaging/v2"
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
	now         time.Time
}

func (this *HandlerFixture) Setup() {
	this.input = make(chan messaging.Delivery, 16)
	this.output = make(chan interface{}, 16)
	this.transformer = &FakeTransformer{}
	this.handler = newHandler(this.input, this.output, this.transformer).WithSleep(0)
	this.now = clock.UTCNow()
	this.handler.clock = clock.Freeze(this.now)
}

func (this *HandlerFixture) TestMessagesHandled() {
	this.input <- messaging.Delivery{Message: 1, Receipt: 11}
	this.input <- messaging.Delivery{Message: 2, Receipt: 12}
	go close(this.input)
	this.handler.Listen()

	this.So(this.transformer.calls, should.Equal, 1)
	this.So(this.transformer.now, should.Equal, this.now)
	this.So(this.transformer.messages, should.Resemble, []interface{}{1, 2})
	this.So(<-this.output, should.Equal, 12)
	this.So(<-this.output, should.BeNil) // channel closed
}

/* ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

type FakeTransformer struct {
	calls    int
	now      time.Time
	messages []interface{}
}

func (this *FakeTransformer) Transform(now time.Time, messages []interface{}) {
	this.calls++
	this.now = now
	this.messages = append(this.messages, messages...)
}
