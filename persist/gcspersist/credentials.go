package gcspersist

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
)

func ParseCredentialsFromJSON(raw []byte) (Credentials, error) {
	type serviceAccount struct {
		PrivateKeyPEM string `json:"private_key"`
		ClientEmail   string `json:"client_email"`
	}

	parsed := serviceAccount{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return Credentials{}, err
	} else {
		return NewCredentials(parsed.ClientEmail, []byte(parsed.PrivateKeyPEM))
	}
}

/* ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

type Credentials struct {
	AccessID   string
	PrivateKey PrivateKey
}

func NewCredentials(accessID string, privateKey []byte) (Credentials, error) {
	if parsed, err := NewPrivateKey(privateKey); err != nil {
		return Credentials{}, err
	} else {
		return Credentials{AccessID: accessID, PrivateKey: parsed}, nil
	}
}

/* ////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

type PrivateKey struct {
	inner *rsa.PrivateKey
}

func NewPrivateKey(raw []byte) (PrivateKey, error) {
	if parsed, err := tryReadPrivateKey(raw); err != nil {
		return PrivateKey{}, err
	} else {
		return PrivateKey{inner: parsed}, err
	}
}
func tryReadPrivateKey(key []byte) (*rsa.PrivateKey, error) {
	if block, _ := pem.Decode(key); block != nil {
		key = block.Bytes
	}

	if parsed, err := tryReadPKCS8(key); err == nil {
		return parsed, nil
	} else {
		return x509.ParsePKCS1PrivateKey(key)
	}
}
func tryReadPKCS8(key []byte) (*rsa.PrivateKey, error) {
	if parsed, err := x509.ParsePKCS8PrivateKey(key); err != nil {
		return nil, err
	} else if parsed, ok := parsed.(*rsa.PrivateKey); !ok {
		return nil, errors.New("malformed private key")
	} else {
		return parsed, nil
	}
}

func (this *PrivateKey) Sign(raw []byte) ([]byte, error) {
	sum := sha256.Sum256(raw)
	return rsa.SignPKCS1v15(rand.Reader, this.inner, crypto.SHA256, sum[:])
}
