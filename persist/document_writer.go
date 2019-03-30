package persist

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/s3"
)

type DocumentWriter struct {
	logger *logging.Logger

	credentials s3.Option
	storage     s3.Option
	client      HTTPClient
}

func NewDocumentWriter(storage *url.URL, accessKey, secretKey string, client HTTPClient) *DocumentWriter {
	return &DocumentWriter{
		credentials: s3.Credentials(accessKey, secretKey),
		storage:     s3.StorageAddress(storage),
		client:      client,
	}
}

func (this *DocumentWriter) Write(document projector.Document) {
	body := this.serialize(document)
	checksum := this.md5Checksum(body)
	request := this.buildRequest(document.Path(), body, checksum)
	response, err := this.client.Do(request)
	this.handleResponse(response, err)
}

func (this *DocumentWriter) serialize(document projector.Document) []byte {
	buffer := bytes.NewBuffer([]byte{})
	gzipWriter, _ := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	encoder := json.NewEncoder(gzipWriter)

	if err := encoder.Encode(document); err != nil {
		this.logger.Panic(err)
	}

	_ = gzipWriter.Close()
	return buffer.Bytes()
}

func (this *DocumentWriter) md5Checksum(body []byte) string {
	sum := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func (this *DocumentWriter) buildRequest(path string, body []byte, checksum string) *http.Request {
	request, err := s3.NewRequest(
		s3.PUT,
		this.credentials,
		this.storage,
		s3.Key(path),
		s3.ContentBytes(body),
		s3.ContentType("application/json"),
		s3.ContentEncoding("gzip"),
		s3.ContentMD5(checksum),
		s3.ServerSideEncryption(s3.ServerSideEncryptionAES256),
	)
	if err != nil {
		this.logger.Panic(err)
	}
	return request
}

// handleResponse handles error response, which technically, shouldn't happen
// because the inner client should be handling retry indefinitely, until the service
// response. This is here merely for the sake of completeness, and to bullet-proof
// the software in case the behavior of the inner client changes in the future.
func (this *DocumentWriter) handleResponse(response *http.Response, err error) {
	if err != nil {
		this.logger.Panic(err)
		return
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		this.logger.Panic(fmt.Errorf("Non-200 HTTP Status Code: %d %s", response.StatusCode, response.Status))
		return
	}
}
