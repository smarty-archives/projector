package transform

import (
	"strconv"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/clock"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/projector"
)

func TestMessageTransformerFixture(t *testing.T) {
	gunit.Run(new(MessageTransformerFixture), t)
}

type MessageTransformerFixture struct {
	*gunit.Fixture

	documents   []projector.Document
	transformer *MessageTransformer
	now         time.Time
}

func (this *MessageTransformerFixture) Setup() {
	this.documents = []projector.Document{&FakeDocument{}}
	this.transformer = NewMessageTransformer(this.documents, nil)
	this.now = clock.UTCNow()
}

func (this *MessageTransformerFixture) SkipTestLapseDocumentOverwritesOriginal() {
	this.transformer.TransformAllDocuments(this.now, "My Message")

	this.So(this.transformer.documents[0].Path(), should.Equal, "1")
}

func (this *MessageTransformerFixture) SkipTestMessagesHandledByDocuments() {
	this.transformer.TransformAllDocuments(this.now, "My Message1", "My Message2")

	fakeDocument := this.transformer.documents[0].(*FakeDocument)
	this.So(fakeDocument.appliedMessages, should.Resemble, []interface{}{"My Message1", "My Message2"})
}

func (this *MessageTransformerFixture) SkipTestNilMessageSkipped() {
	this.transformer.TransformAllDocuments(this.now, nil)

	fakeDocument := this.transformer.documents[0].(*FakeDocument)
	this.So(fakeDocument.applies, should.Equal, 0)
}

// //////////////////////////////////////////////////////////

type FakeDocument struct {
	skip            bool
	depth           int
	applies         int
	appliedMessages []interface{}
	lapsed          time.Time
}

func (this *FakeDocument) Path() string {
	return strconv.Itoa(this.depth)
}
func (this *FakeDocument) Lapse(now time.Time) projector.Document {
	this.lapsed = now
	return &FakeDocument{skip: this.skip, depth: this.depth + 1, applies: this.applies, appliedMessages: this.appliedMessages}
}
func (this *FakeDocument) Apply(message interface{}) bool {
	if this.skip {
		return false
	}

	this.applies++
	this.appliedMessages = append(this.appliedMessages, message)
	return true
}
func (this *FakeDocument) Reset()                 {}
func (this *FakeDocument) SetVersion(interface{}) {}
func (this *FakeDocument) Version() interface{}   { return "etag" }
