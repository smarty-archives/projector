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
	cloner      *FakeCloner
	now         time.Time
}

func (this *MessageTransformerFixture) Setup() {
	this.documents = []projector.Document{&FakeDocument{}}
	this.cloner = &FakeCloner{}
	this.transformer = NewMessageTransformer(this.documents, this.cloner)
	this.now = clock.UTCNow()
}

// //////////////////////////////////////////////////////////

func (this *MessageTransformerFixture) TestLapseDocumentOverwritesOriginal() {
	this.transformer.TransformAllDocuments(this.now, "My Message")

	this.So(this.transformer.documents[0].Path(), should.Equal, "1")
}

// //////////////////////////////////////////////////////////

func (this *MessageTransformerFixture) TestMessagesHandledByDocuments() {
	this.transformer.TransformAllDocuments(this.now, "My Message1", "My Message2")

	fakeDocument := this.transformer.documents[0].(*FakeDocument)
	this.So(fakeDocument.appliedMessages, should.Resemble, []interface{}{"My Message1", "My Message2"})
}

// //////////////////////////////////////////////////////////

func (this *MessageTransformerFixture) TestNilMessageSkipped() {
	this.transformer.TransformAllDocuments(this.now, nil)

	fakeDocument := this.transformer.documents[0].(*FakeDocument)
	this.So(fakeDocument.applies, should.Equal, 0)
}

// //////////////////////////////////////////////////////////

func (this *MessageTransformerFixture) TestCollectReturnsClonedDocuments() {
	transformer := NewMessageTransformer(nil, this.cloner)
	transformer.changed["/path/to/doc/1"] = &FakeDocument{}
	transformer.changed["/path/to/doc/2"] = &FakeDocument{}

	for _, item := range transformer.Collect() {
		this.So(item.(*FakeDocument).depth, should.Equal, 1)
	}
}

// //////////////////////////////////////////////////////////

func (this *MessageTransformerFixture) TestMultipleCollectsOnlyReturnsOnce() {
	transformer := NewMessageTransformer(nil, this.cloner)
	transformer.changed["/path/to/doc/1"] = &FakeDocument{}

	this.So(transformer.Collect(), should.NotBeEmpty)
	this.So(transformer.Collect(), should.BeEmpty)
}

// //////////////////////////////////////////////////////////

type FakeDocument struct {
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
	return &FakeDocument{depth: this.depth + 1, applies: this.applies, appliedMessages: this.appliedMessages}
}
func (this *FakeDocument) Apply(message interface{}) bool {
	this.applies++
	this.appliedMessages = append(this.appliedMessages, message)
	return true
}

// //////////////////////////////////////////////////////////

type FakeCloner struct {
}

func (this *FakeCloner) Clone(document projector.Document) projector.Document {
	document.(*FakeDocument).depth++
	return document
}
