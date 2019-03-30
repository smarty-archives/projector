package persist

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
	"github.com/smartystreets/logging"
	"github.com/smartystreets/nu"
	"github.com/smartystreets/projector"
)

func TestS3WriterFixture(t *testing.T) {
	gunit.Run(new(S3WriterFixture), t)
}

type S3WriterFixture struct {
	*gunit.Fixture
	client *FakeHTTPClientForWriting
	writer *S3Writer
}

func (this *S3WriterFixture) Setup() {
	this.client = NewFakeHTTPClientForWriting()
	address := nu.URLParsed("https://bucket.s3-us-west-1.amazonaws.com/")
	this.writer = NewS3Writer(address, "access", "secret", this.client)
	this.writer.logger = logging.Capture()
}

// /////////////////////////////////////////////////////////////////

func (this *S3WriterFixture) TestDocumentIsTranslatedToAnHTTPRequest() {
	this.writer.Write(writableDocument)
	this.So(this.client.received, should.NotBeNil)
	this.So(this.client.received.URL.Path, should.Equal, writableDocument.Path())
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

func (this *S3WriterFixture) TestDocumentWithIncompatibleFieldCausesPanicUponSerialization() {
	action := func() { this.writer.Write(badJSONDocument) }
	this.So(action, should.PanicWith, "json: unsupported type: chan int")
}

// /////////////////////////////////////////////////////////////////

func (this *S3WriterFixture) TestThatInnerClientFailureCausesPanic() {
	this.client.err = errors.New("Failure")
	action := func() { this.writer.Write(writableDocument) }
	this.So(action, should.PanicWith, this.client.err.Error())
}

// /////////////////////////////////////////////////////////////////

func (this *S3WriterFixture) TestThatInnerClientUnsuccessfulCausesPanic() {
	this.client.statusCode = http.StatusInternalServerError
	this.client.statusMessage = "Internal Server Error"
	action := func() { this.writer.Write(writableDocument) }
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
	return &http.Response{
		StatusCode: this.statusCode,
		Status:     this.statusMessage,
		Body:       this.responseBody,
	}, this.err
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
func (this *DocumentForWriting) Path() string                                  { return "/this/is/the/path.json" }

// ///////////////////////////////////////////////////////////////

var badJSONDocument = &BadJSONDocumentForWriting{}

// Maps must have string keys to be JSON serialized.
type BadJSONDocumentForWriting struct{ Stuff chan int }

func (this *BadJSONDocumentForWriting) Lapse(now time.Time) (next projector.Document) { return this }
func (this *BadJSONDocumentForWriting) Apply(message interface{}) bool                { return false }
func (this *BadJSONDocumentForWriting) Path() string                                  { return "" }

// ///////////////////////////////////////////////////////////////

var badPathDocument = &BadPathDocumentForWriting{path: "%%%%%%%%"}

type BadPathDocumentForWriting struct{ path string }

func (this *BadPathDocumentForWriting) Lapse(now time.Time) (next projector.Document) { return this }
func (this *BadPathDocumentForWriting) Apply(message interface{}) bool                { return false }
func (this *BadPathDocumentForWriting) Path() string                                  { return this.path }
