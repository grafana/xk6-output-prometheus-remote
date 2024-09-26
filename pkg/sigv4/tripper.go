package sigv4

import (
	"errors"
	"net/http"
)

type Tripper struct {
	config *Config
	signer Signer
	next   http.RoundTripper
}

type Config struct {
	Region             string
	AwsSecretAccessKey string
	AwsAccessKeyID     string
}

func NewRoundTripper(config *Config, next http.RoundTripper) (*Tripper, error) {
	if config == nil {
		return nil, errors.New("can't initialize a sigv4 round tripper with nil config")
	}

	if next == nil {
		next = http.DefaultTransport
	}

	tripper := &Tripper{
		config: config,
		next:   next,
		signer: NewDefaultSigner(config),
	}
	return tripper, nil
}

func (c *Tripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := c.signer.Sign(req); err != nil {
		return nil, err
	}
	return c.next.RoundTrip(req)
}
