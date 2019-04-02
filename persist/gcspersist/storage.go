package gcspersist

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
	"google.golang.org/api/option"
)

type ReadWriter struct {
	context    context.Context
	client     *storage.Client
	bucket     string
	pathPrefix string
	logger     *logging.Logger
}

func NewReadWriter(ctx context.Context, bucketName, pathPrefix string, httpClient *http.Client, serviceAccountKey []byte) (*ReadWriter, error) {
	options := []option.ClientOption{option.WithCredentialsJSON(serviceAccountKey), option.WithHTTPClient(httpClient)}
	if client, err := storage.NewClient(ctx, options...); err != nil {
		return nil, err
	} else {
		return newReadWriter(ctx, client, bucketName, pathPrefix), nil
	}
}
func newReadWriter(ctx context.Context, client *storage.Client, bucket string, pathPrefix string) *ReadWriter {
	return &ReadWriter{
		context:    ctx,
		client:     client,
		bucket:     bucket,
		pathPrefix: pathPrefix,
	}
}

func (this *ReadWriter) Name() string { return "Google Cloud Storage" }

func (this *ReadWriter) Read(document projector.Document) error {
	reader, err := this.client.
		Bucket(this.bucket).
		Object(this.normalizeFilename(document.Path())).
		NewReader(this.context)

	if storage.ErrObjectNotExist == err {
		this.logger.Printf("[INFO] Document not found at '%s'\n", document.Path())
		return nil
	}

	defer func() { _ = reader.Close() }()

	if err != nil {
		return err
	}

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(document); err != nil {
		return fmt.Errorf("document read error: '%s'", err.Error())
	}

	document.SetVersion(reader.Attrs.Generation)
	return nil
}
func (this *ReadWriter) ReadPanic(document projector.Document) {
	if err := this.Read(document); err != nil {
		this.logger.Panic(err)
	}
}

func (this *ReadWriter) Write(document projector.Document) error {
	var generation int64
	if value, ok := document.Version().(int64); ok {
		generation = value
	}

	conditions := storage.Conditions{
		GenerationMatch: generation,
		DoesNotExist:    generation == 0,
	}

	filename := path.Join(this.pathPrefix, document.Path())
	writer := this.client.
		Bucket(this.bucket).
		Object(this.normalizeFilename(filename)).
		If(conditions).
		NewWriter(this.context)

	body := serialize(document)
	writer.ContentType = "application/json"
	writer.ContentEncoding = "gzip"
	writer.MD5 = md5Checksum(body.Bytes())

	if _, err := io.Copy(writer, body); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return wrapError(err)
	}

	document.SetVersion(writer.Attrs().Generation)
	return nil
}
func (this *ReadWriter) normalizeFilename(value string) string {
	value = path.Join(this.pathPrefix, value)
	for strings.HasPrefix(value, "/") {
		value = value[1:]
	}
	return value
}

func serialize(document projector.Document) *bytes.Buffer {
	buffer := bytes.NewBuffer([]byte{})
	gzipWriter, _ := gzip.NewWriterLevel(buffer, gzip.BestCompression)
	encoder := json.NewEncoder(gzipWriter)

	if err := encoder.Encode(document); err != nil {
		panic(err)
	}

	_ = gzipWriter.Close()
	return buffer
}

func md5Checksum(body []byte) []byte {
	sum := md5.Sum(body)
	return sum[:]
}
func wrapError(err error) error {
	message := err.Error()
	if strings.Contains(message, "412") && strings.Contains(message, "Precondition Failed") {
		return persist.ErrConcurrentWrite
	} else {
		return err
	}
}
