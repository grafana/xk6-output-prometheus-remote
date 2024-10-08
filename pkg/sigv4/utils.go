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

// stripExcessSpaces will remove the leading and trailing spaces, and side-by-side spaces are converted
// into a single space.
func stripExcessSpaces(str string) string {
	if idx := strings.Index(str, "  "); idx < 0 {
		return str
	}

	builder := strings.Builder{}
	lastFoundSpace := -1
	const space = ' '
	str = strings.TrimSpace(str)
	for i := 0; i < len(str); i++ {
		if str[i] == space {
			lastFoundSpace = i
			continue
		}

		if lastFoundSpace > 0 && builder.Len() != 0 {
			builder.WriteByte(space)
		}
		builder.WriteByte(str[i])
		lastFoundSpace = -1
	}
	return builder.String()
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
