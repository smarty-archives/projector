package gcs

import (
	"net/url"
	"path"
	"time"
)

type Request struct {
	signer     Signer
	accessID   string
	objectKey  string
	expires    string
	expiration time.Time
}

func NewRequest(method, bucket, objectKey string, expiration time.Time, credentials Credentials) (Request, error) {
	objectKey = path.Join("/", bucket, objectKey)

	signer, err := NewSigner(method, objectKey, "", nil, expiration, credentials.PrivateKey)
	if err != nil {
		return Request{}, err
	}

	return Request{
		signer:     signer,
		accessID:   credentials.AccessID,
		objectKey:  objectKey,
		expires:    signer.Epoch(),
		expiration: expiration,
	}, nil
}

func (this Request) SignedURL() (string, error) {
	if signature, err := this.signer.Calculate(); err != nil {
		return "", err
	} else {
		return this.buildSignedURL(signature), nil
	}
}
func (this Request) buildSignedURL(signature string) string {
	targetURL := &url.URL{Scheme: "https", Host: "storage.googleapis.com", Path: this.objectKey}
	query := targetURL.Query()
	query.Set("GoogleAccessId", this.accessID)
	query.Set("Expires", this.expires)
	query.Set("Signature", signature)
	targetURL.RawQuery = query.Encode()
	return targetURL.String()
}
