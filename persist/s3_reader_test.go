package persist

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/nu"
)

func TestS3Fixture(t *testing.T) {
	gunit.Run(new(S3Fixture), t)
}

type S3Fixture struct {
	*gunit.Fixture

	path     string
	reader   *S3Reader
	client   *FakeHTTPGetClient // HTTPClient
	document *Document
}

func (this *S3Fixture) Setup() {
	this.path = "/document/path"
	this.client = &FakeHTTPGetClient{}
	address := nu.URLParsed("https://bucket.s3-us-west-1.amazonaws.com/")
	this.reader = NewS3Reader(address, "access", "secret", this.client)
	this.reader.logger = logging.Capture()
	this.document = &Document{}
}

func (this *S3Fixture) TestClientErrorPreventsDocumentReading() {
	this.client.err = errors.New("BOINK!")
	this.assertPanic("HTTP Client Error: BOINK!")
}

func (this *S3Fixture) TestDocumentNotFound_JSONMarshalNotAttempted() {
	this.client.response = &http.Response{StatusCode: 404, Body: newHTTPBody("Not found")}
	this.read()
	this.So(this.document.ID, should.Equal, 0)
}

func (this *S3Fixture) TestBodyUnreadable() {
	var bodyUnreadableResponse = &http.Response{StatusCode: 200, Body: newReadErrorHTTPBody()}
	this.client.response = bodyUnreadableResponse
	this.So(func() { this.read() }, should.Panic)
	this.So(this.document.ID, should.Equal, 0)
	this.So(bodyUnreadableResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}

func (this *S3Fixture) TestBadJSON() {
	var badJSONResponse = &http.Response{StatusCode: 200, Body: newHTTPBody("I am bad JSON.")}
	this.client.response = badJSONResponse
	this.So(func() { this.read() }, should.Panic)
	this.So(this.document.ID, should.Equal, 0)
	this.So(badJSONResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}

func (this *S3Fixture) TestValidUncompressedResponse_PopulatesDocument() {
	var validUncompressedResponse = &http.Response{StatusCode: 200, Body: newHTTPBody(`{"ID": 1234}`)}
	this.client.response = validUncompressedResponse
	this.read()
	this.So(this.client.request.URL.Path, should.Equal, "/document/path")
	this.So(this.document.ID, should.Equal, 1234)
	this.So(validUncompressedResponse.Body.(*FakeHTTPResponseBody).closed, should.BeTrue)
}
func (this *S3Fixture) TestValidCompressedResponse_PopulatesDocument() {
	var validCompressedResponse = &http.Response{StatusCode: 200, Body: newHTTPBody(`{"ID": 1234}`)}

	validCompressedResponse.Header = make(http.Header)
	validCompressedResponse.Header.Set("Content-Encoding", "gzip")
	validCompressedResponse.Header.Set("Etag", "abc1234")

	targetBuffer := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(targetBuffer)
	_, _ = io.Copy(writer, validCompressedResponse.Body)
	_ = writer.Close()

	validCompressedResponse.Body = ioutil.NopCloser(targetBuffer)

	this.client.response = validCompressedResponse
	etag := this.read()
	this.So(this.document.ID, should.Equal, 1234)
	this.So(etag, should.Equal, "abc1234")
}
func (this *S3Fixture) read() interface{} {
	return this.reader.ReadPanic(this.path, this.document)
}
func (this *S3Fixture) assertPanic(message string) {
	this.So(func() { this.read() }, should.Panic)
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
