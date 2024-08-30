package sigv4

import (
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

func NewRoundTripper(config *Config, next http.RoundTripper) *Tripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return &Tripper{
		config: config,
		next:   next,
		signer: NewDefaultSigner(config),
	}
}

func (c *Tripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := c.signer.Sign(req); err != nil {
		return nil, err
	}
	return c.next.RoundTrip(req)
}
