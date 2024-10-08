package sigv4

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTripper_request_includes_required_headers(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if required headers are present
		authorization := r.Header.Get(authorizationHeaderKey)
		amzDate := r.Header.Get(amzDateKey)
		contentSHA256 := r.Header.Get(contentSHAKey)

		// Respond to the request
		w.WriteHeader(http.StatusOK)

		assert.NotEmptyf(t, authorization, "%s header should be present", authorizationHeaderKey)
		assert.NotEmptyf(t, amzDate, "%s header should be present", amzDateKey)
		assert.NotEmpty(t, contentSHA256, "%s header should be present", contentSHAKey)
	}))
	defer server.Close()

	client := http.Client{}
	tripper, err := NewRoundTripper(&Config{
		Region:             "us-east1",
		AwsSecretAccessKey: "xyz",
		AwsAccessKeyID:     "abc",
	}, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = tripper

	req, err := http.NewRequest("POST", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	client.Do(req)
}
