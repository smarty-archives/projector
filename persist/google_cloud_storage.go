package persist

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

type GoogleCloudStorage struct {
	context    context.Context
	client     *storage.Client
	bucket     string
	pathPrefix string
	logger     *logging.Logger
}

func NewGoogleCloudStorage(ctx context.Context, client *storage.Client, bucket string, pathPrefix string) *GoogleCloudStorage {
	return &GoogleCloudStorage{
		context:    ctx,
		client:     client,
		bucket:     bucket,
		pathPrefix: pathPrefix,
	}
}

func (this *GoogleCloudStorage) Read(filename string, document interface{}) error {
	filename = path.Join(this.pathPrefix, filename)
	reader, _ := this.client.
		Bucket(this.bucket).
		Object(filename).
		NewReader(this.context)

	defer func() { _ = reader.Close() }()

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(document); err != nil {
		return fmt.Errorf("Document read error: '%s'", err.Error())
	}

	return nil // TODO: change read signature to return generation or make document actually be a projector.Document with methods on it to store the generation
}
func (this *GoogleCloudStorage) ReadPanic(path string, document interface{}) {
	if err := this.Read(path, document); err != nil {
		this.logger.Panic(err)
	}
}

func (this *GoogleCloudStorage) Write(document projector.Document) {
	generation := int64(0) // TODO: grab generation from Document
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

	_, _ = io.Copy(writer, body) // TODO: precondition failure, etc.
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
