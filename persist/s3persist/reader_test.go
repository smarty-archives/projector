package s3persist

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
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

func TestReaderFixture(t *testing.T) {
	gunit.Run(new(ReaderFixture), t)
}

type ReaderFixture struct {
	*gunit.Fixture

	reader   *Reader
	client   *FakeHTTPGetClient // HTTPClient
	document *Document
}

func (this *ReaderFixture) Setup() {
	this.client = &FakeHTTPGetClient{}
	address := nu.URLParsed("https://bucket.s3-us-west-1.amazonaws.com/")
	this.reader = NewReader(address, "access", "secret", this.client)
	this.reader.logger = logging.Capture()
	this.document = &Document{}
}

func (this *ReaderFixture) TestClientErrorPreventsDocumentReading() {
	this.client.err = errors.New("BOINK!")
	this.assertPanic("HTTP Client Error: BOINK!")
}

func (this *ReaderFixture) TestDocumentNotFound_JSONMarshalNotAttempted() {
	this.client.response = &http.Response{StatusCode: 404, Body: newHTTPBody("Not found")}
	this.read()
	this.So(this.document.ID, should.Equal, 0)
}

func (this *ReaderFixture) TestBodyUnreadable() {
	var bodyUnreadableResponse = &http.Response{StatusCode: 200, Body: newReadErrorHTTPBody()}
	this.client.response = bodyUnreadableResponse
	this.So(func() { this.read() }, should.Panic)
	this.So(this.document.ID, should.Equal, 0)
	this.So(bodyUnreadableResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}

func (this *ReaderFixture) TestBadJSON() {
	var badJSONResponse = &http.Response{StatusCode: 200, Body: newHTTPBody("I am bad JSON.")}
	this.client.response = badJSONResponse
	this.So(func() { this.read() }, should.Panic)
	this.So(this.document.ID, should.Equal, 0)
	this.So(badJSONResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}

func (this *ReaderFixture) TestValidUncompressedResponse_PopulatesDocument() {
	var validUncompressedResponse = &http.Response{StatusCode: 200, Body: newHTTPBody(`{"ID": 1234}`)}
	this.client.response = validUncompressedResponse
	this.read()
	this.So(this.client.request.URL.Path, should.Equal, this.document.Path())
	this.So(this.document.ID, should.Equal, 1234)
	this.So(validUncompressedResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}
func (this *ReaderFixture) TestValidCompressedResponse_PopulatesDocument() {
	var validCompressedResponse = &http.Response{StatusCode: 200, Body: newHTTPBody(`{"ID": 1234}`)}

	validCompressedResponse.Header = make(http.Header)
	validCompressedResponse.Header.Set("Content-Encoding", "gzip")
	validCompressedResponse.Header.Set("ETag", "abc1234")

	targetBuffer := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(targetBuffer)
	_, _ = io.Copy(writer, validCompressedResponse.Body)
	_ = writer.Close()

	validCompressedResponse.Body = ioutil.NopCloser(targetBuffer)

	this.client.response = validCompressedResponse
	this.read()
	this.So(this.document.ID, should.Equal, 1234)
}
func (this *ReaderFixture) read() {
	this.reader.ReadPanic(this.document)
}
func (this *ReaderFixture) assertPanic(message string) {
	this.So(this.read, should.Panic)
	this.So(this.document.ID, should.Equal, 0)
}

// ////////////////////////////////////////////////////////////////////////////////////////////

type FakeHTTPGetClient struct {
	err      error
	response *http.Response
	request  *http.Request
}

func (this *FakeHTTPGetClient) Do(request *http.Request) (*http.Response, error) {
	this.request = request
	return this.response, this.err
}

// /////////////////////////////////////////////////////////////////////////////////////////

type Document struct{ ID int }

func (this *Document) Lapse(now time.Time) (next projector.Document) { return this }
func (this *Document) Apply(message interface{}) bool                { return false }
func (this *Document) Path() string                                  { return "/this/is/the/path.json" }
func (this *Document) Reset()                                        {}
func (this *Document) SetVersion(interface{})                        {}
func (this *Document) Version() interface{}                          { return "etag" }

// //////////////////////////////////////////////////////////////////////////////////////////

func newHTTPBody(message string) io.ReadCloser {
	return &FakeHTTPResponseBody{Reader: strings.NewReader(message)}
}
func newReadErrorHTTPBody() io.ReadCloser {
	return &FakeHTTPResponseBody{err: errors.New("BOINK!")}
}

type FakeHTTPResponseBody struct {
	*strings.Reader

	err    error
	closed bool
}

func (this *FakeHTTPResponseBody) Read(p []byte) (int, error) {
	if this.err != nil {
		return 0, this.err
	}
	return this.Reader.Read(p)
}

func (this *FakeHTTPResponseBody) Close() error {
	this.closed = true
	return nil
}
