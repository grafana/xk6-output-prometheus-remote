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
	AwsSecretAccessKey string
	AwsAccessKeyID     string
}

func NewRoundTripper(config *Config, next http.RoundTripper) (*Tripper, error) {
	if config == nil {
		return nil, errors.New("can't initialize a sigv4 round tripper with nil config")
	}

	if len(strings.TrimSpace(config.Region)) == 0 {
		return nil, errors.New("sigV4 config `Region` must be set")
	}

	if len(strings.TrimSpace(config.AwsSecretAccessKey)) == 0 {
		return nil, errors.New("sigV4 config `AwsSecretAccessKey` must be set")
	}

	if len(strings.TrimSpace(config.AwsAccessKeyID)) == 0 {
		return nil, errors.New("sigV4 config `AwsAccessKeyID` must be set")
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
