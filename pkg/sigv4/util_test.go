package sigv4

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStripExcessSpaces(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		arg  string
		want string
	}{
		{
			arg:  `AWS4-HMAC-SHA256 Credential=AKIDFAKEIDFAKEID/20160628/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=1234567890abcdef1234567890abcdef1234567890abcdef`,
			want: `AWS4-HMAC-SHA256 Credential=AKIDFAKEIDFAKEID/20160628/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=1234567890abcdef1234567890abcdef1234567890abcdef`,
		},
		{
			arg:  "a   b   c   d",
			want: "a b c d",
		},
		{
			arg:  "   abc   def   ghi   jk   ",
			want: "abc def ghi jk",
		},
		{
			arg:  "   123    456    789          101112   ",
			want: "123 456 789 101112",
		},
		{
			arg:  "12     3       1abc123",
			want: "12 3 1abc123",
		},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, stripExcessSpaces(tc.arg))
	}

}
