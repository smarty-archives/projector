package gcspersist

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"path"

	"cloud.google.com/go/storage"
	"github.com/smartystreets/logging"
	"github.com/smartystreets/projector"
)

type ReadWriter struct {
	context    context.Context
	client     *storage.Client
	bucket     string
	pathPrefix string
	logger     *logging.Logger
}

func NewReadWriter(ctx context.Context, client *storage.Client, bucket string, pathPrefix string) *ReadWriter {
	return &ReadWriter{
		context:    ctx,
		client:     client,
		bucket:     bucket,
		pathPrefix: pathPrefix,
	}
}

func (this *ReadWriter) Read(filename string, document interface{}) (interface{}, error) {
	filename = path.Join(this.pathPrefix, filename)
	reader, _ := this.client.
		Bucket(this.bucket).
		Object(filename).
		NewReader(this.context)

	defer func() { _ = reader.Close() }()

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(document); err != nil {
		return 0, fmt.Errorf("document read error: '%s'", err.Error())
	}

	return 0, nil
}
func (this *ReadWriter) ReadPanic(path string, document interface{}) interface{} {
	if etag, err := this.Read(path, document); err != nil {
		this.logger.Panic(err)
		return 0
	} else {
		return etag
	}
}

func (this *ReadWriter) Write(document projector.Document) (interface{}, error) {
	generation := document.Version().(int64)
	conditions := storage.Conditions{
		GenerationMatch: generation,
		DoesNotExist:    generation == 0,
	}

	filename := path.Join(this.pathPrefix, document.Path())
	writer := this.client.
		Bucket(this.bucket).
		Object(filename).
		If(conditions).
		NewWriter(this.context)

	body := serialize(document)
	writer.ContentType = "application/json"
	writer.ContentEncoding = "gzip"
	writer.MD5 = md5Checksum(body.Bytes())

	if _, err := io.Copy(writer, body); err != nil {
		return 0, err
	}

	if err := writer.Close(); err != nil {
		return 0, err
	}

	return writer.Attrs().Generation, nil
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
