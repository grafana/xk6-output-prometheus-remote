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
	"strconv"
	"strings"
	"time"
)

type Signer interface {
	Sign(req *http.Request) error
}

type DefaultSigner struct {
	config *Config

	// noEscape represents the characters that AWS doesn't escape
	noEscape [256]bool
}

func NewDefaultSigner(config *Config) Signer {
	ds := &DefaultSigner{
		config:   config,
		noEscape: [256]bool{},
	}

	for i := 0; i < len(ds.noEscape); i++ {
		// AWS expects every character except these to be escaped
		ds.noEscape[i] = (i >= 'A' && i <= 'Z') ||
			(i >= 'a' && i <= 'z') ||
			(i >= '0' && i <= '9') ||
			i == '-' ||
			i == '.' ||
			i == '_' ||
			i == '~'
	}

	return ds
}

func (d *DefaultSigner) Sign(req *http.Request) error {
	now := time.Now().UTC()
	iSO8601Date := now.Format(timeFormat)

	credentialScope := buildCredentialScope(now, d.config.Region)

	payloadHash, err := d.getPayloadHash(req)
	if err != nil {
		return err
	}

	req.Header.Set("Host", req.Host)
	req.Header.Set(amzDateKey, iSO8601Date)
	req.Header.Set(contentSHAKey, payloadHash)

	_, signedHeadersStr, canonicalHeaderStr := buildCanonicalHeaders(req)

	canonicalQueryString := getCanonicalQueryString(req.URL)
	canonicalReq := buildCanonicalString(
		req.Method,
		getCanonicalURI(req.URL, d.noEscape),
		canonicalQueryString,
		canonicalHeaderStr,
		signedHeadersStr,
		payloadHash,
	)

	signature := sign(
		deriveKey(d.config.AwsSecretAccessKey, d.config.Region),
		buildStringToSign(iSO8601Date, credentialScope, canonicalReq),
	)

	authorizationHeader := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		signingAlgorithm,
		d.config.AwsAccessKeyID,
		credentialScope,
		signedHeadersStr,
		signature,
	)

	req.URL.RawQuery = canonicalQueryString
	req.Header.Set(authorizationHeaderKey, authorizationHeader)
	return nil
}

func (d *DefaultSigner) getPayloadHash(req *http.Request) (string, error) {
	if req.Body == nil {
		return emptyStringSHA256, nil
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

func buildCredentialScope(signingTime time.Time, region string) string {
	return fmt.Sprintf(
		"%s/%s/%s/aws4_request",
		signingTime.UTC().Format(shortTimeFormat),
		region,
		awsServiceName,
	)
}

func buildCanonicalString(method, uri, query, canonicalHeaders, signedHeaders, payloadHash string) string {
	return strings.Join([]string{
		method,
		uri,
		query,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
}

var ignoredHeaders = map[string]struct{}{
	"Authorization":   struct{}{},
	"User-Agent":      struct{}{},
	"X-Amzn-Trace-Id": struct{}{},
	"Expect":          struct{}{},
}

// buildCanonicalHeaders is mostly ported from https://github.com/aws/aws-sdk-go-v2/aws/signer/v4 buildCanonicalHeaders
func buildCanonicalHeaders(req *http.Request) (signed http.Header, signedHeaders, canonicalHeadersStr string) {
	host, header, length := req.Host, req.Header, req.ContentLength

	signed = make(http.Header)

	var headers []string
	const hostHeader = "host"
	headers = append(headers, hostHeader)
	signed[hostHeader] = append(signed[hostHeader], host)

	const contentLengthHeader = "content-length"
	if length > 0 {
		headers = append(headers, contentLengthHeader)
		signed[contentLengthHeader] = append(signed[contentLengthHeader], strconv.FormatInt(length, 10))
	}

	for k, v := range header {
		if _, ok := ignoredHeaders[k]; ok {
			continue // ignored header
		}
		if strings.EqualFold(k, contentLengthHeader) {
			// prevent signing already handled content-length header.
			continue
		}

		lowerCaseKey := strings.ToLower(k)
		if _, ok := signed[lowerCaseKey]; ok {
			// include additional values
			signed[lowerCaseKey] = append(signed[lowerCaseKey], v...)
			continue
		}

		headers = append(headers, lowerCaseKey)
		signed[lowerCaseKey] = v
	}
	sort.Strings(headers)

	signedHeaders = strings.Join(headers, ";")

	var canonicalHeaders strings.Builder
	n := len(headers)
	const colon = ':'
	for i := 0; i < n; i++ {
		if headers[i] == hostHeader {
			canonicalHeaders.WriteString(hostHeader)
			canonicalHeaders.WriteRune(colon)
			canonicalHeaders.WriteString(stripExcessSpaces(host))
		} else {
			canonicalHeaders.WriteString(headers[i])
			canonicalHeaders.WriteRune(colon)
			// Trim out leading, trailing, and dedup inner spaces from signed header values.
			values := signed[headers[i]]
			for j, v := range values {
				cleanedValue := strings.TrimSpace(stripExcessSpaces(v))
				canonicalHeaders.WriteString(cleanedValue)
				if j < len(values)-1 {
					canonicalHeaders.WriteRune(',')
				}
			}
		}
		canonicalHeaders.WriteRune('\n')
	}
	canonicalHeadersStr = canonicalHeaders.String()

	return signed, signedHeaders, canonicalHeadersStr
}

func getCanonicalURI(u *url.URL, noEscape [256]bool) string {
	return escapePath(getURIPath(u), false, noEscape)
}

func getCanonicalQueryString(u *url.URL) string {
	query := u.Query()

	// Sort Each Query Key's Values
	for key := range query {
		sort.Strings(query[key])
	}

	var rawQuery strings.Builder
	rawQuery.WriteString(strings.Replace(query.Encode(), "+", "%20", -1))
	return rawQuery.String()
}

func buildStringToSign(amzDate, credentialScope, canonicalRequestString string) string {
	hash := sha256.New()
	hash.Write([]byte(canonicalRequestString))
	return strings.Join([]string{
		signingAlgorithm,
		amzDate,
		credentialScope,
		hex.EncodeToString(hash.Sum(nil)),
	}, "\n")
}

func deriveKey(secretKey, region string) string {
	signingDate := time.Now().UTC().Format(shortTimeFormat)
	hmacDate := hmacSHA256([]byte("AWS4"+secretKey), signingDate)
	hmacRegion := hmacSHA256(hmacDate, region)
	hmacService := hmacSHA256(hmacRegion, awsServiceName)
	signingKey := hmacSHA256(hmacService, "aws4_request")
	return string(signingKey)
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sign(signingKey string, strToSign string) string {
	h := hmac.New(sha256.New, []byte(signingKey))
	h.Write([]byte(strToSign))
	sig := hex.EncodeToString(h.Sum(nil))
	return sig
}
