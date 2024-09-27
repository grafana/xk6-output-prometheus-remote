package sigv4

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
)

// escapePath escapes part of a URL path in Amazon style.
// inspired by github.com/aws/smithy-go/encoding/httpbinding EscapePath method
func escapePath(path string, encodeSep bool, noEscape [256]bool) string {
	var buf bytes.Buffer
	for i := 0; i < len(path); i++ {
		c := path[i]
		if noEscape[c] || (c == '/' && !encodeSep) {
			buf.WriteByte(c)
		} else {
			fmt.Fprintf(&buf, "%%%02X", c)
		}
	}
	return buf.String()
}

// stripExcessSpaces will rewrite the passed in slice's string values to not
// contain multiple side-by-side spaces.
// Ported from github.com/aws/aws-sdk-go-v2/aws/signer/internal/v4 StripExcessSpaces
func stripExcessSpaces(str string) string {
	const doubleSpace = "  "

	var j, k, l, m, spaces int
	// Trim trailing spaces
	for j = len(str) - 1; j >= 0 && str[j] == ' '; j-- {
	}

	// Trim leading spaces
	for k = 0; k < j && str[k] == ' '; k++ {
	}
	str = str[k : j+1]

	// Strip multiple spaces.
	j = strings.Index(str, doubleSpace)
	if j < 0 {
		return str
	}

	buf := []byte(str)
	for k, m, l = j, j, len(buf); k < l; k++ {
		if buf[k] == ' ' {
			if spaces == 0 {
				// First space.
				buf[m] = buf[k]
				m++
			}
			spaces++
		} else {
			// End of multiple spaces.
			spaces = 0
			buf[m] = buf[k]
			m++
		}
	}

	return string(buf[:m])
}

// getURIPath returns the escaped URI component from the provided URL.
// Ported from github.com/aws/aws-sdk-go-v2/aws/signer/internal/v4 GetURIPath
func getURIPath(u *url.URL) string {
	var uriPath string

	if len(u.Opaque) > 0 {
		const schemeSep, pathSep, queryStart = "//", "/", "?"

		opaque := u.Opaque
		// Cut off the query string if present.
		if idx := strings.Index(opaque, queryStart); idx >= 0 {
			opaque = opaque[:idx]
		}

		// Cutout the scheme separator if present.
		if strings.Index(opaque, schemeSep) == 0 {
			opaque = opaque[len(schemeSep):]
		}

		// capture URI path starting with first path separator.
		if idx := strings.Index(opaque, pathSep); idx >= 0 {
			uriPath = opaque[idx:]
		}
	} else {
		uriPath = u.EscapedPath()
	}

	if len(uriPath) == 0 {
		uriPath = "/"
	}

	return uriPath
}
