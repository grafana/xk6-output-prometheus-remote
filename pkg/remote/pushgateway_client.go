package remote

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"net/http"
	"net/url"
)

type PushgatewayClient struct {
	hc  *http.Client
	url *url.URL
	job string
	cfg *HTTPConfig
}

type RegistryPusher interface {
	Push(ctx context.Context, registries []*prometheus.Registry) error
}

var _ RegistryPusher = new(PushgatewayClient)

func NewPushgatewayClient(endpoint string, job string, cfg *HTTPConfig) (*PushgatewayClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	pgwc := &PushgatewayClient{
		hc: &http.Client{
			Timeout: cfg.Timeout,
		},
		url: u,
		job: job,
		cfg: cfg,
	}

	if cfg.TLSConfig != nil {
		pgwc.hc.Transport = &http.Transport{
			TLSClientConfig: cfg.TLSConfig,
		}
	}

	return pgwc, nil
}

func (pgwc *PushgatewayClient) Push(ctx context.Context, registries []*prometheus.Registry) error {
	pusher := push.New(pgwc.url.String(), pgwc.job)

	header := http.Header{}
	if len(pgwc.cfg.Headers) > 0 {
		header = pgwc.cfg.Headers.Clone()
	}
	header.Set("User-Agent", "k6-prometheus-rw-output")
	pusher.Header(header)

	if pgwc.cfg.BasicAuth != nil {
		pusher.BasicAuth(pgwc.cfg.BasicAuth.Username, pgwc.cfg.BasicAuth.Password)
	}

	for _, registry := range registries {
		pusher.Gatherer(registry)
	}

	if err := pusher.AddContext(ctx); err != nil {
		return fmt.Errorf("could not push metrics to pushgateway: %w", err)
	}

	return nil
}
