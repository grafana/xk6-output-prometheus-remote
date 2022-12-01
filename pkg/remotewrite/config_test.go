package remotewrite

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"
)

func TestConfigApply(t *testing.T) {
	t.Parallel()

	fullConfig := Config{
		URL:                   null.StringFrom("some-url"),
		InsecureSkipTLSVerify: null.BoolFrom(false),
		Username:              null.StringFrom("user"),
		Password:              null.StringFrom("pass"),
		PushInterval:          types.NullDurationFrom(10 * time.Second),
		Headers: map[string]string{
			"X-Header": "value",
		},
		TrendStats: []string{"p(99)"},
	}

	// Defaults should be overwritten by valid values
	c := NewConfig()
	c = c.Apply(fullConfig)
	assert.Equal(t, fullConfig, c)

	// Defaults shouldn't be impacted by invalid values
	c = NewConfig()
	c = c.Apply(Config{
		Username:              null.NewString("user", false),
		Password:              null.NewString("pass", false),
		InsecureSkipTLSVerify: null.NewBool(false, false),
	})
	assert.Equal(t, false, c.Username.Valid)
	assert.Equal(t, false, c.Password.Valid)
	assert.Equal(t, true, c.InsecureSkipTLSVerify.Valid)
}

func TestConfigRemoteConfig(t *testing.T) {
	u, err := url.Parse("https://prometheus.ie/remote")
	require.NoError(t, err)

	config := Config{
		URL:                   null.StringFrom(u.String()),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		Username:              null.StringFrom("myuser"),
		Password:              null.StringFrom("mypass"),
		Headers: map[string]string{
			"X-MYCUSTOM-HEADER": "val1",
		},
	}

	headers := http.Header{}
	headers.Set("X-MYCUSTOM-HEADER", "val1")
	exprcc := &remote.HTTPConfig{
		Timeout: 5 * time.Second,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		BasicAuth: &remote.BasicAuth{
			Username: "myuser",
			Password: "mypass",
		},
		Headers: headers,
	}
	rcc, err := config.RemoteConfig()
	require.NoError(t, err)
	assert.Equal(t, exprcc, rcc)
}

func TestGetConsolidatedConfig(t *testing.T) {
	t.Parallel()

	u, err := url.Parse("https://prometheus.ie/remote")
	require.NoError(t, err)

	testCases := map[string]struct {
		jsonRaw   json.RawMessage
		env       map[string]string
		arg       string
		config    Config
		errString string
	}{
		"Defaults": {
			jsonRaw: nil,
			env:     nil,
			arg:     "",
			config: Config{
				URL:                   null.StringFrom("http://localhost:9090/api/v1/write"),
				InsecureSkipTLSVerify: null.BoolFrom(true),
				Username:              null.NewString("", false),
				Password:              null.NewString("", false),
				PushInterval:          types.NullDurationFrom(5 * time.Second),
				Headers:               make(map[string]string),
				TrendStats:            []string{"p(99)"},
			},
		},
		"JSONSuccess": {
			jsonRaw: json.RawMessage(fmt.Sprintf(`{"url":"%s"}`, u.String())),
			config: Config{
				URL:                   null.StringFrom(u.String()),
				InsecureSkipTLSVerify: null.BoolFrom(true),
				Username:              null.NewString("", false),
				Password:              null.NewString("", false),
				PushInterval:          types.NullDurationFrom(defaultPushInterval),
				Headers:               make(map[string]string),
				TrendStats:            []string{"p(99)"},
			},
		},
		"MixedSuccess": {
			jsonRaw: json.RawMessage(fmt.Sprintf(`{"url":"%s"}`, u.String())),
			env: map[string]string{
				"K6_PROMETHEUS_INSECURE_SKIP_TLS_VERIFY": "false",
				"K6_PROMETHEUS_USER":                     "u",
			},
			arg: "username=user",
			config: Config{
				URL:                   null.StringFrom(u.String()),
				InsecureSkipTLSVerify: null.BoolFrom(false),
				Username:              null.NewString("user", true),
				Password:              null.NewString("", false),
				PushInterval:          types.NullDurationFrom(defaultPushInterval),
				Headers:               make(map[string]string),
				TrendStats:            []string{"p(99)"},
			},
		},
		"OrderOfPrecedence": {
			jsonRaw: json.RawMessage(`{"url":"http://json:9090","username":"json","password":"json"}`),
			env: map[string]string{
				"K6_PROMETHEUS_USERNAME": "env",
				"K6_PROMETHEUS_PASSWORD": "env",
			},
			arg: "password=arg",
			config: Config{
				URL:                   null.StringFrom("http://json:9090"),
				InsecureSkipTLSVerify: null.BoolFrom(true),
				Username:              null.StringFrom("env"),
				Password:              null.StringFrom("arg"),
				PushInterval:          types.NullDurationFrom(defaultPushInterval),
				Headers:               make(map[string]string),
				TrendStats:            []string{"p(99)"},
			},
		},
		"InvalidJSON": {
			jsonRaw:   json.RawMessage(`{"invalid-json "astring"}`),
			errString: "parse JSON options failed",
		},
		"InvalidEnv": {
			env:       map[string]string{"K6_PROMETHEUS_INSECURE_SKIP_TLS_VERIFY": "d"},
			errString: "parse environment variables options failed",
		},
		"InvalidArg": {
			arg:       "insecureSkipTLSVerify=wrongtime",
			errString: "parse argument string options failed",
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(testCase.jsonRaw, testCase.env, testCase.arg)
			if len(testCase.errString) > 0 {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), testCase.errString)
				return
			}
			assert.Equal(t, testCase.config, c)
		})
	}
}

func TestParseURL(t *testing.T) {
	t.Parallel()

	c, err := parseArg("url=http://prometheus.remote:3412/write")
	assert.Nil(t, err)
	assert.Equal(t, null.StringFrom("http://prometheus.remote:3412/write"), c.URL)

	c, err = parseArg("url=http://prometheus.remote:3412/write,insecureSkipTLSVerify=false,pushInterval=2s")
	assert.Nil(t, err)
	assert.Equal(t, null.StringFrom("http://prometheus.remote:3412/write"), c.URL)
	assert.Equal(t, null.BoolFrom(false), c.InsecureSkipTLSVerify)
	assert.Equal(t, types.NullDurationFrom(time.Second*2), c.PushInterval)

	c, err = parseArg("headers.X-Header=value")
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"X-Header": "value"}, c.Headers)
}

func TestOptionURL(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"url":"http://prometheus:9090/api/v1/write"}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_REMOTE_URL": "http://prometheus:9090/api/v1/write"}},
		"Arg":  {arg: "url=http://prometheus:9090/api/v1/write"},
	}

	expconfig := Config{
		URL:                   null.StringFrom("http://prometheus:9090/api/v1/write"),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		Username:              null.NewString("", false),
		Password:              null.NewString("", false),
		PushInterval:          types.NullDurationFrom(5 * time.Second),
		Headers:               make(map[string]string),
		TrendStats:            []string{"p(99)"},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestOptionHeaders(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"headers":{"X-MY-HEADER1":"hval1","X-MY-HEADER2":"hval2"}}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_HEADERS_X-MY-HEADER1": "hval1", "K6_PROMETHEUS_HEADERS_X-MY-HEADER2": "hval2"}},
		"Arg":  {arg: "headers.X-MY-HEADER1=hval1,headers.X-MY-HEADER2=hval2"},
	}

	expconfig := Config{
		URL:                   null.StringFrom("http://localhost:9090/api/v1/write"),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		PushInterval:          types.NullDurationFrom(5 * time.Second),
		Headers: map[string]string{
			"X-MY-HEADER1": "hval1",
			"X-MY-HEADER2": "hval2",
		},
		TrendStats: []string{"p(99)"},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestOptionInsecureSkipTLSVerify(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"insecureSkipTLSVerify":false}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_INSECURE_SKIP_TLS_VERIFY": "false"}},
		"Arg":  {arg: "insecureSkipTLSVerify=false"},
	}

	expconfig := Config{
		URL:                   null.StringFrom(defaultURL),
		InsecureSkipTLSVerify: null.BoolFrom(false),
		PushInterval:          types.NullDurationFrom(defaultPushInterval),
		Headers:               make(map[string]string),
		TrendStats:            []string{"p(99)"},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestOptionBasicAuth(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"username":"user1","password":"pass1"}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_USERNAME": "user1", "K6_PROMETHEUS_PASSWORD": "pass1"}},
		"Arg":  {arg: "username=user1,password=pass1"},
	}

	expconfig := Config{
		URL:                   null.StringFrom("http://localhost:9090/api/v1/write"),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		Username:              null.StringFrom("user1"),
		Password:              null.StringFrom("pass1"),
		PushInterval:          types.NullDurationFrom(5 * time.Second),
		Headers:               make(map[string]string),
		TrendStats:            []string{"p(99)"},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestOptionTrendAsNativeHistogram(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"trendAsNativeHistogram":true}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_TREND_AS_NATIVE_HISTOGRAM": "true"}},
		"Arg":  {arg: "trendAsNativeHistogram=true"},
	}

	expconfig := Config{
		URL:                    null.StringFrom("http://localhost:9090/api/v1/write"),
		InsecureSkipTLSVerify:  null.BoolFrom(true),
		Username:               null.NewString("", false),
		Password:               null.NewString("", false),
		PushInterval:           types.NullDurationFrom(5 * time.Second),
		Headers:                make(map[string]string),
		TrendAsNativeHistogram: null.BoolFrom(true),
		TrendStats:             []string{"p(99)"},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestOptionPushInterval(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"pushInterval":"1m2s"}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_PUSH_INTERVAL": "1m2s"}},
		"Arg":  {arg: "pushInterval=1m2s"},
	}

	expconfig := Config{
		URL:                   null.StringFrom("http://localhost:9090/api/v1/write"),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		Username:              null.NewString("", false),
		Password:              null.NewString("", false),
		PushInterval:          types.NullDurationFrom((1 * time.Minute) + (2 * time.Second)),
		Headers:               make(map[string]string),
		TrendStats:            []string{"p(99)"},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}

func TestConfigTrendStats(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		arg     string
		env     map[string]string
		jsonRaw json.RawMessage
	}{
		"JSON": {jsonRaw: json.RawMessage(`{"trendStats":["max","p(95)"]}`)},
		"Env":  {env: map[string]string{"K6_PROMETHEUS_TREND_STATS": "max,p(95)"}},
		// TODO: support arg, check the comment in the code
		//"Arg":  {arg: "trendStats=max,p(95)"},
	}

	expconfig := Config{
		URL:                   null.StringFrom("http://localhost:9090/api/v1/write"),
		InsecureSkipTLSVerify: null.BoolFrom(true),
		PushInterval:          types.NullDurationFrom(5 * time.Second),
		Headers:               make(map[string]string),
		TrendStats:            []string{"max", "p(95)"},
	}

	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			c, err := GetConsolidatedConfig(
				tc.jsonRaw, tc.env, tc.arg)
			require.NoError(t, err)
			assert.Equal(t, expconfig, c)
		})
	}
}
