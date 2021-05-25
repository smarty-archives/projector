package s3persist

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/projector"
)

func TestWriterFixture(t *testing.T) {
	gunit.Run(new(WriterFixture), t)
}

type WriterFixture struct {
	*gunit.Fixture
	client *FakeHTTPClientForWriting
	writer *Writer
}

func (this *WriterFixture) Setup() {
	this.client = NewFakeHTTPClientForWriting()
	address := urlParsed("https://bucket.s3-us-west-1.amazonaws.com/")
	this.writer = NewWriter(address, "access", "secret", this.client)
}

// /////////////////////////////////////////////////////////////////

func (this *WriterFixture) TestDocumentIsTranslatedToAnHTTPRequest() {
	_ = this.writer.Write(writableDocument)
	this.So(this.client.received, should.NotBeNil)
	this.So(this.client.received.URL.Path, should.EndWith, writableDocument.Path())
	this.So(this.client.received.Method, should.Equal, "PUT")
	body, _ := ioutil.ReadAll(this.client.received.Body)
	this.So(decodeBody(body), should.Equal, `{"Message":"Hello, World!"}`)
	this.So(this.client.received.ContentLength, should.Equal, len(body))
	this.So(this.client.received.Header.Get("Content-Encoding"), should.Equal, "gzip")
	this.So(this.client.received.Header.Get("Content-Type"), should.Equal, "application/json")
	this.So(this.client.received.Header.Get("Content-MD5"), should.NotBeBlank)
	this.So(this.client.received.Header.Get("x-amz-server-side-encryption"), should.NotBeBlank)
	this.So(this.client.responseBody.closed, should.Equal, 1)
}
func decodeBody(body []byte) string {
	buffer := bytes.NewReader(body)
	reader, _ := gzip.NewReader(buffer)
	decoded, _ := ioutil.ReadAll(reader)
	return strings.TrimSpace(string(decoded))
}

// /////////////////////////////////////////////////////////////////

func (this *WriterFixture) TestDocumentWithIncompatibleFieldCausesPanicUponSerialization() {
	action := func() { _ = this.writer.Write(badJSONDocument) }
	this.So(action, should.PanicWith, "json: unsupported type: chan int")
}

// /////////////////////////////////////////////////////////////////

func (this *WriterFixture) TestThatInnerClientFailureCausesPanic() {
	this.client.err = errors.New("Failure")
	action := func() { _ = this.writer.Write(writableDocument) }
	this.So(action, should.PanicWith, this.client.err.Error())
}

// /////////////////////////////////////////////////////////////////

func (this *WriterFixture) TestThatInnerClientUnsuccessfulCausesPanic() {
	this.client.statusCode = http.StatusInternalServerError
	this.client.statusMessage = "Internal Server Error"
	action := func() { _ = this.writer.Write(writableDocument) }
	this.So(action, should.PanicWith, "Non-200 HTTP Status Code: 500 Internal Server Error")
}

// /////////////////////////////////////////////////////////////////

type FakeHTTPClientForWriting struct {
	received      *http.Request
	responseBody  *FakeBody
	err           error
	statusCode    int
	statusMessage string
}

func NewFakeHTTPClientForWriting() *FakeHTTPClientForWriting {
	return &FakeHTTPClientForWriting{
		statusCode:   http.StatusOK,
		responseBody: &FakeBody{},
	}
}
func (this *FakeHTTPClientForWriting) Do(request *http.Request) (*http.Response, error) {
	this.received = request

	response := &http.Response{
		StatusCode: this.statusCode,
		Status:     this.statusMessage,
		Body:       this.responseBody,
	}

	response.Header = make(http.Header)
	response.Header.Set("ETag", "etag-here")

	return response, this.err
}

// ///////////////////////////////////////////////////////////////

type FakeBody struct{ closed int }

func (this *FakeBody) Read([]byte) (int, error) { return 0, nil }
func (this *FakeBody) Close() error             { this.closed++; return nil }

// ///////////////////////////////////////////////////////////////

var writableDocument = &DocumentForWriting{Message: "Hello, World!"}

type DocumentForWriting struct{ Message string }

func (this *DocumentForWriting) Lapse(now time.Time) (next projector.Document) { return this }
func (this *DocumentForWriting) Apply(message interface{}) bool                { return false }
func (this *DocumentForWriting) Path() string                                  { return "/bucket/this/is/the/path.json" }
func (this *DocumentForWriting) Reset()                                        {}
func (this *DocumentForWriting) SetVersion(interface{})                        {}
func (this *DocumentForWriting) Version() interface{}                          { return "etag" }

// ///////////////////////////////////////////////////////////////

var badJSONDocument = &BadJSONDocumentForWriting{}

// Maps must have string keys to be JSON serialized.
type BadJSONDocumentForWriting struct{ Stuff chan int }

func (this *BadJSONDocumentForWriting) Lapse(now time.Time) (next projector.Document) { return this }
func (this *BadJSONDocumentForWriting) Apply(message interface{}) bool                { return false }
func (this *BadJSONDocumentForWriting) Path() string                                  { return "" }
func (this *BadJSONDocumentForWriting) Reset()                                        {}
func (this *BadJSONDocumentForWriting) SetVersion(interface{})                        {}
func (this *BadJSONDocumentForWriting) Version() interface{}                          { return "etag" }
