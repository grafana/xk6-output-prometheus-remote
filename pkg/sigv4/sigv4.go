package sigv4

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const signingAlgo = "AWS4-HMAC-SHA256"
const awsServiceName = "aps"

type Signer interface {
	Sign(req *http.Request) error
}

type DefaultSigner struct {
	iSO8601Date      string
	canonicalHeaders string
	signedHeaders    string
	credentialScope  string
	config           *Config
	payloadHash      string
}

func NewDefaultSigner(config *Config) Signer {
	return &DefaultSigner{
		config: config,
	}
}

func (d *DefaultSigner) Sign(req *http.Request) error {
	now := time.Now().UTC()
	d.iSO8601Date = now.Format("20060102T150405Z")
	d.credentialScope = fmt.Sprintf(
		"%s/%s/%s/aws4_request",
		now.UTC().Format("20060102"),
		d.config.Region,
		awsServiceName,
	)

	payloadHash, err := d.getPayloadHash(req)
	if err != nil {
		return err
	}

	d.payloadHash = payloadHash
	d.addRequiredHeaders(req)
	d.canonicalHeaders, d.signedHeaders = d.getCanonicalAndSignedHeaders(req)

	canonicalReq := d.createCanonicalRequest(req)
	stringToSign, err := d.createStringToSign(canonicalReq)
	if err != nil {
		return err
	}

	signature := d.sign(d.createSigningKey(), stringToSign)
	authorizationHeader := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		signingAlgo,
		d.config.AwsAccessKeyID,
		d.credentialScope,
		d.signedHeaders,
		signature,
	)
	req.Header.Set("Authorization", authorizationHeader)
	return nil
}

func (d *DefaultSigner) getPayloadHash(req *http.Request) (string, error) {
	if req.Body == nil {
		return hex.EncodeToString(sha256.New().Sum(nil)), nil
	}

	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	reqBodyBuffer := bytes.NewReader(reqBody)

	hash := sha256.New()
	if _, err := io.Copy(hash, reqBodyBuffer); err != nil {
		return "", err
	}

	payloadHash := hex.EncodeToString(hash.Sum(nil))

	// ensuring that we keep the request body intact for next tripper
	req.Body = io.NopCloser(bytes.NewReader(reqBody))

	return payloadHash, nil
}

func (d *DefaultSigner) addRequiredHeaders(req *http.Request) {
	req.Header.Set("Host", req.Host)
	req.Header.Set("x-amz-date", d.iSO8601Date)
	req.Header.Set("x-amz-content-sha256", d.payloadHash)
}

func (d *DefaultSigner) getCanonicalAndSignedHeaders(req *http.Request) (string, string) {
	var headers []string
	var signedHeaders []string

	for key, value := range req.Header {
		lowercaseKey := strings.ToLower(key)
		encodedValue := strings.TrimSpace(strings.Join(value, ","))
		headers = append(headers, lowercaseKey+":"+encodedValue)
		signedHeaders = append(signedHeaders, lowercaseKey)
	}

	sort.Strings(headers)
	sort.Strings(signedHeaders)

	canonicalHeaders := strings.Join(headers, "\n") + "\n"
	canonicalSignedHeaders := strings.Join(signedHeaders, ";")
	return canonicalHeaders, canonicalSignedHeaders
}

func (d *DefaultSigner) createCanonicalRequest(req *http.Request) string {
	return strings.Join([]string{
		req.Method,
		d.getCanonicalURI(req.URL),
		d.getCanonicalQueryString(req.URL),
		d.canonicalHeaders,
		d.signedHeaders,
		d.payloadHash,
	}, "\n")
}

func (d *DefaultSigner) getCanonicalURI(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}

	// The spec requires not to encode `/`
	segments := strings.Split(u.Path, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}

	return strings.Join(segments, "/")
}

func (d *DefaultSigner) getCanonicalQueryString(u *url.URL) string {
	queryParams := u.Query()
	var queryPairs []string

	for key, values := range queryParams {
		for _, value := range values {
			queryPairs = append(queryPairs, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}

	sort.Strings(queryPairs)

	return strings.Join(queryPairs, "&")
}

func (d *DefaultSigner) createStringToSign(canonicalRequest string) (string, error) {
	hash := sha256.New()
	if _, err := hash.Write([]byte(canonicalRequest)); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s\n%s\n%s\n%s",
		signingAlgo,
		d.iSO8601Date,
		d.credentialScope,
		hex.EncodeToString(hash.Sum(nil)),
	), nil
}

func (d *DefaultSigner) createSigningKey() string {
	signingDate := time.Now().UTC().Format("20060102")
	dateKey := d.hmacSHA256([]byte("AWS4"+d.config.AwsSecretAccessKey), signingDate)
	dateRegionKey := d.hmacSHA256(dateKey, d.config.Region)
	dateRegionServiceKey := d.hmacSHA256(dateRegionKey, awsServiceName)
	signingKey := d.hmacSHA256(dateRegionServiceKey, "aws4_request")
	return string(signingKey)
}

func (d *DefaultSigner) hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func (d *DefaultSigner) sign(signingKey string, strToSign string) string {
	h := hmac.New(sha256.New, []byte(signingKey))
	h.Write([]byte(strToSign))
	sig := hex.EncodeToString(h.Sum(nil))
	return sig
}
