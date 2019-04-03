package gcs

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"time"
)

type Signer struct {
	Method      string
	Path        string
	ContentType string
	ContentMD5  []byte
	Expiration  time.Time
	Key         PrivateKey
}

func NewSigner(method string, fullPath, contentType string, contentMD5 []byte, expiration time.Time, key PrivateKey) (Signer, error) {
	return Signer{
		Method:      method,
		Path:        fullPath,
		ContentType: contentType,
		ContentMD5:  contentMD5,
		Expiration:  expiration,
		Key:         key,
	}, nil
}

func (this Signer) Calculate() (string, error) {
	buffer := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buffer, "%s\n", this.Method)
	_, _ = fmt.Fprintf(buffer, "%s\n", this.ContentMD5)
	_, _ = fmt.Fprintf(buffer, "%s\n", this.ContentType)
	_, _ = fmt.Fprintf(buffer, "%s\n", this.Epoch())
	_, _ = fmt.Fprintf(buffer, "%s", this.Path)

	if signed, err := this.Key.Sign(buffer.Bytes()); err != nil {
		return "", err
	} else {
		return base64.StdEncoding.EncodeToString(signed), nil
	}
}

func (this Signer) Epoch() string {
	return fmt.Sprintf("%d", this.Expiration.Unix())
}
