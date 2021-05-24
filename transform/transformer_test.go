package transform

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

func TestTransformerFixture(t *testing.T) {
	gunit.Run(new(TransformerFixture), t)
}

type TransformerFixture struct {
	*gunit.Fixture

	messages    []interface{}
	now         time.Time
	documents   []*FakeDocument
	store       *FakeStorage
	transformer Transformer
}

func (this *TransformerFixture) Setup() {
	this.messages = []interface{}{"1", 2, 3.0}
	this.now = utcNow()
	this.store = NewFakeStorage()

	var docs []projector.Document
	for i := 0; i < 10; i++ {
		this.documents = append(this.documents, &FakeDocument{index: i})
		docs = append(docs, this.documents[i])
	}
	this.transformer = newTransformer(this.store, docs...)
}

func (this *TransformerFixture) TestAllDocumentsTransformedAndWritten() {
	this.transformer.Transform(this.now, this.messages)

	var applyTimes []time.Time
	for _, document := range this.documents {
		applyTimes = append(applyTimes, document.applyTime)
		this.So(document.reset, should.BeZeroValue)
		this.So(document.apply, should.Equal, len(this.messages))
		this.So(document.now, should.Equal, this.now)
		this.So(document.applyTime.After(this.now), should.BeTrue) // apply happens after call to Lapse
		this.So(document.messages, should.Resemble, this.messages)
		this.So(this.store.writes["/"+fmt.Sprint(document.index)], should.Equal, document)
	}

	this.So(this.store.reads, should.BeEmpty)
	this.So(applyTimes, should.NotBeChronological)
}

func (this *TransformerFixture) TestFailedWriteRetried() {
	document := &FakeDocument{}
	this.documents = []*FakeDocument{document}
	this.transformer = newTransformer(this.store, document)
	this.store.writeErrorCount = 1 // failure on the first write and success thereafter

	this.transformer.Transform(this.now, this.messages)

	this.So(document.reset, should.Equal, 1)
	this.So(this.store.writeCount, should.Equal, 2)
	this.So(this.store.writes[document.Path()], should.Equal, document)
	this.So(this.store.reads[document.Path()], should.Equal, document)
}

/* ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

type FakeStorage struct {
	mutex           sync.Mutex
	reads           map[string]projector.Document
	writes          map[string]projector.Document
	writeCount      int
	writeErrorCount int
}

func NewFakeStorage() *FakeStorage {
	return &FakeStorage{reads: map[string]projector.Document{}, writes: map[string]projector.Document{}}
}
func (this *FakeStorage) Name() string                          { panic("nop") }
func (this *FakeStorage) ReadPanic(document projector.Document) { panic("nop") }
func (this *FakeStorage) Read(document projector.Document) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.reads[document.Path()] = document
	return nil
}
func (this *FakeStorage) Write(document projector.Document) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.writes[document.Path()] = document

	if this.writeCount++; this.writeCount >= this.writeErrorCount+1 {
		return nil
	} else {
		return persist.ErrConcurrentWrite
	}
}

type FakeDocument struct {
	index     int
	apply     int
	reset     int
	applyTime time.Time
	now       time.Time
	messages  []interface{}
	version   interface{}
}

func (this *FakeDocument) Apply(message interface{}) bool {
	this.apply++
	this.applyTime = utcNow()
	this.messages = append(this.messages, message)
	return true
}
func (this *FakeDocument) Lapse(now time.Time) (next projector.Document) { this.now = now; return this }
func (this *FakeDocument) Path() string                                  { return fmt.Sprintf("/%d", this.index) }
func (this *FakeDocument) Reset()                                        { this.reset++ }
func (this *FakeDocument) SetVersion(value interface{})                  { this.version = value }
func (this *FakeDocument) Version() interface{}                          { panic("nop") }
func utcNow() time.Time                                                  { return time.Now().UTC() }
