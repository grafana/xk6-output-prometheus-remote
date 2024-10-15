package sigv4

import (
	"errors"
	"net/http"
	"strings"
)

type Tripper struct {
	config *Config
	signer signer
	next   http.RoundTripper
}

type Config struct {
	Region             string
	AwsAccessKeyID     string
	AwsSecretAccessKey string
}

func (c *Config) validate() error {
	if c == nil {
		return errors.New("config should not be nil")
	}
	hasRegion := len(strings.TrimSpace(c.Region)) != 0
	hasAccessID := len(strings.TrimSpace(c.AwsAccessKeyID)) != 0
	hasSecretAccessKey := len(strings.TrimSpace(c.AwsSecretAccessKey)) != 0
	if !hasRegion || !hasAccessID || !hasSecretAccessKey {
		return errors.New("sigV4 config `Region`, `AwsAccessKeyID`, `AwsSecretAccessKey` must all be set")
	}
	return nil
}

func NewRoundTripper(config *Config, next http.RoundTripper) (*Tripper, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if next == nil {
		next = http.DefaultTransport
	}

	tripper := &Tripper{
		config: config,
		next:   next,
		signer: newDefaultSigner(config),
	}
	return tripper, nil
}

func (c *Tripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := c.signer.sign(req); err != nil {
		return nil, err
	}
	return c.next.RoundTrip(req)
}
