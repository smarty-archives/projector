package gcspersist

import (
	"net/url"
	"path"
	"time"
)

type Request struct {
	documentPath string
	signer       Signer
	expiration   time.Time
	expires      string
	settings     StorageSettings
}

func NewRequest(method string, documentPath string, expiration time.Time, settings StorageSettings) (Request, error) {
	signer, err := NewSigner(method, settings.BucketName, settings.PathPrefix, documentPath,
		"", nil, expiration, settings.Credentials.PrivateKey)

	if err != nil {
		return Request{}, err
	}

	return Request{
		signer:       signer,
		documentPath: documentPath,
		expiration:   expiration,
		expires:      signer.Epoch(),
		settings:     settings,
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
	targetURL := &url.URL{
		Scheme: "https",
		Host:   "storage.googleapis.com",
		Path:   path.Join("/", this.settings.BucketName, this.settings.PathPrefix, this.documentPath),
	}
	query := targetURL.Query()
	query.Set("GoogleAccessId", this.settings.Credentials.AccessID)
	query.Set("Expires", this.expires)
	query.Set("Signature", signature)
	targetURL.RawQuery = query.Encode()
	return targetURL.String()
}
