package remotewrite

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/remote"
	"github.com/kubernetes/helm/pkg/strvals"

	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"
)

const (
	defaultURL          = "http://localhost:9090/api/v1/write"
	defaultTimeout      = 5 * time.Second
	defaultPushInterval = 5 * time.Second
	defaultMetricPrefix = "k6_"
)

type Config struct {
	// URL contains the absolute URL for the Write endpoint where to flush the time series.
	URL null.String `json:"url" envconfig:"K6_PROMETHEUS_REMOTE_URL"`

	// Headers contains additional headers that should be included in the HTTP requests.
	Headers map[string]string `json:"headers" envconfig:"K6_PROMETHEUS_HEADERS"`

	// InsecureSkipTLSVerify skips TLS client side checks.
	InsecureSkipTLSVerify null.Bool `json:"insecureSkipTLSVerify" envconfig:"K6_PROMETHEUS_INSECURE_SKIP_TLS_VERIFY"`

	// Username is the User for Basic Auth.
	Username null.String `json:"username" envconfig:"K6_PROMETHEUS_USERNAME"`

	// Password is the Password for the Basic Auth.
	Password null.String `json:"password" envconfig:"K6_PROMETHEUS_PASSWORD"`

	// PushInterval defines the time between flushes. The Output will wait the set time
	// before push a new set of time series to the endpoint.
	PushInterval types.NullDuration `json:"pushInterval" envconfig:"K6_PROMETHEUS_PUSH_INTERVAL"`
}

func NewConfig() Config {
	return Config{
		URL:                   null.StringFrom(defaultURL),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		Username:              null.NewString("", false),
		Password:              null.NewString("", false),
		PushInterval:          types.NullDurationFrom(defaultPushInterval),
		Headers:               make(map[string]string),
	}
}

func (conf Config) RemoteConfig() (*remote.HTTPConfig, error) {
	hc := remote.HTTPConfig{
		Timeout: defaultTimeout,
	}

	// if at least valid user was configured, use basic auth
	if conf.Username.Valid {
		hc.BasicAuth = &remote.BasicAuth{
			Username: conf.Username.String,
			Password: conf.Password.String,
		}
	}

	hc.TLSConfig = &tls.Config{
		InsecureSkipVerify: conf.InsecureSkipTLSVerify.Bool,
	}

	// TODO: consider if the auth logic should be enforced here
	// (e.g. if insecureSkipTLSVerify is switched off, then check for non-empty certificate file and auth, etc.)

	if len(conf.Headers) > 0 {
		hc.Headers = make(http.Header)
		for k, v := range conf.Headers {
			hc.Headers.Add(k, v)
		}
	}
	return &hc, nil
}

// From here till the end of the file partial duplicates waiting for config refactor (k6 #883)

func (base Config) Apply(applied Config) Config {
	if applied.URL.Valid {
		base.URL = applied.URL
	}

	if applied.InsecureSkipTLSVerify.Valid {
		base.InsecureSkipTLSVerify = applied.InsecureSkipTLSVerify
	}

	if applied.Username.Valid {
		base.Username = applied.Username
	}

	if applied.Password.Valid {
		base.Password = applied.Password
	}

	if applied.PushInterval.Valid {
		base.PushInterval = applied.PushInterval
	}

	if len(applied.Headers) > 0 {
		for k, v := range applied.Headers {
			base.Headers[k] = v
		}
	}

	return base
}

// ParseArg takes an arg string and converts it to a config
func ParseArg(arg string) (Config, error) {
	var c Config
	params, err := strvals.Parse(arg)
	if err != nil {
		return c, err
	}

	if v, ok := params["url"].(string); ok {
		c.URL = null.StringFrom(v)
	}

	if v, ok := params["insecureSkipTLSVerify"].(bool); ok {
		c.InsecureSkipTLSVerify = null.BoolFrom(v)
	}

	if v, ok := params["user"].(string); ok {
		c.Username = null.StringFrom(v)
	}

	if v, ok := params["password"].(string); ok {
		c.Password = null.StringFrom(v)
	}

	if v, ok := params["pushInterval"].(string); ok {
		if err := c.PushInterval.UnmarshalText([]byte(v)); err != nil {
			return c, err
		}
	}

	c.Headers = make(map[string]string)
	if v, ok := params["headers"].(map[string]interface{}); ok {
		for k, v := range v {
			if v, ok := v.(string); ok {
				c.Headers[k] = v
			}
		}
	}

	return c, nil
}

// GetConsolidatedConfig combines {default config values + JSON config +
// environment vars + arg config values}, and returns the final result.
func GetConsolidatedConfig(jsonRawConf json.RawMessage, env map[string]string, arg string) (Config, error) {
	result := NewConfig()
	if jsonRawConf != nil {
		jsonConf := Config{}
		if err := json.Unmarshal(jsonRawConf, &jsonConf); err != nil {
			return result, err
		}
		result = result.Apply(jsonConf)
	}

	getEnvBool := func(env map[string]string, name string) (null.Bool, error) {
		if v, vDefined := env[name]; vDefined {
			if b, err := strconv.ParseBool(v); err != nil {
				return null.NewBool(false, false), err
			} else {
				return null.BoolFrom(b), nil
			}
		}
		return null.NewBool(false, false), nil
	}

	getEnvMap := func(env map[string]string, prefix string) map[string]string {
		result := make(map[string]string)
		for ek, ev := range env {
			if strings.HasPrefix(ek, prefix) {
				k := strings.TrimPrefix(ek, prefix)
				result[k] = ev
			}
		}
		return result
	}

	// envconfig is not processing some undefined vars (at least duration) so apply them manually
	if pushInterval, pushIntervalDefined := env["K6_PROMETHEUS_PUSH_INTERVAL"]; pushIntervalDefined {
		if err := result.PushInterval.UnmarshalText([]byte(pushInterval)); err != nil {
			return result, err
		}
	}

	if url, urlDefined := env["K6_PROMETHEUS_REMOTE_URL"]; urlDefined {
		result.URL = null.StringFrom(url)
	}

	if b, err := getEnvBool(env, "K6_PROMETHEUS_INSECURE_SKIP_TLS_VERIFY"); err != nil {
		return result, err
	} else {
		if b.Valid {
			// apply only if valid, to keep default option otherwise
			result.InsecureSkipTLSVerify = b
		}
	}

	if user, userDefined := env["K6_PROMETHEUS_USER"]; userDefined {
		result.Username = null.StringFrom(user)
	}

	if password, passwordDefined := env["K6_PROMETHEUS_PASSWORD"]; passwordDefined {
		result.Password = null.StringFrom(password)
	}

	envHeaders := getEnvMap(env, "K6_PROMETHEUS_HEADERS_")
	for k, v := range envHeaders {
		result.Headers[k] = v
	}

	if arg != "" {
		argConf, err := ParseArg(arg)
		if err != nil {
			return result, err
		}

		result = result.Apply(argConf)
	}

	return result, nil
}
