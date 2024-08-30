package sigv4_test

import (
	"github.com/grafana/xk6-output-prometheus-remote/pkg/sigv4"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTripper_request_includes_required_headers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if required headers are present
		authorization := r.Header.Get("Authorization")
		amzDate := r.Header.Get("x-amz-date")
		contentSHA256 := r.Header.Get("x-amz-content-sha256")

		// Respond to the request
		w.WriteHeader(http.StatusOK)

		assert.NotEmpty(t, authorization, "Authorization header should be present")
		assert.NotEmpty(t, amzDate, "x-amz-date header should be present")
		assert.NotEmpty(t, contentSHA256, "x-amz-content-sha256 header should be present")
	}))
	defer server.Close()
	client := http.Client{}
	client.Transport = sigv4.NewRoundTripper(&sigv4.Config{}, http.DefaultTransport)

	req, err := http.NewRequest("POST", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	client.Do(req)
}
