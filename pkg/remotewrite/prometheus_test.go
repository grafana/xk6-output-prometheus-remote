package remotewrite

import (
	"fmt"
	"testing"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/tsdb"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"gopkg.in/guregu/null.v3"
)

func TestTagsToLabels(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		tags   map[string]string
		config Config
		labels []prompb.Label
	}{
		"empty-tags": {
			tags: nil,
			config: Config{
				KeepTags:    null.BoolFrom(true),
				KeepNameTag: null.BoolFrom(false),
			},
			labels: []prompb.Label{},
		},
		"name-tag-discard": {
			tags: map[string]string{"foo": "bar", "name": "nnn"},
			config: Config{
				KeepTags:    null.BoolFrom(true),
				KeepNameTag: null.BoolFrom(false),
			},
			labels: []prompb.Label{
				{Name: "foo", Value: "bar"},
			},
		},
		"name-tag-keep": {
			tags: map[string]string{"foo": "bar", "name": "nnn"},
			config: Config{
				KeepTags:    null.BoolFrom(true),
				KeepNameTag: null.BoolFrom(true),
			},
			labels: []prompb.Label{
				{Name: "foo", Value: "bar"},
				{Name: "name", Value: "nnn"},
			},
		},
		"url-tag-discard": {
			tags: map[string]string{"foo": "bar", "url": "uuu"},
			config: Config{
				KeepTags:   null.BoolFrom(true),
				KeepURLTag: null.BoolFrom(false),
			},
			labels: []prompb.Label{
				{Name: "foo", Value: "bar"},
			},
		},
		"url-tag-keep": {
			tags: map[string]string{"foo": "bar", "url": "uuu"},
			config: Config{
				KeepTags:   null.BoolFrom(true),
				KeepURLTag: null.BoolFrom(true),
			},
			labels: []prompb.Label{
				{Name: "foo", Value: "bar"},
				{Name: "url", Value: "uuu"},
			},
		},
		"discard-tags": {
			tags: map[string]string{"foo": "bar", "name": "nnn"},
			config: Config{
				KeepTags: null.BoolFrom(false),
			},
			labels: []prompb.Label{},
		},
	}

	for name, testCase := range testCases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			var tagset tsdb.TagSet
			if testCase.tags != nil {
				for k, v := range testCase.tags {
					tagset = append(tagset, &tsdb.Tag{Key: k, Value: v})
				}
			}
			labels := tagsToLabels(testCase.config, tagset)

			assert.Equal(t, len(testCase.labels), len(labels))

			for i := range testCase.labels {
				var found bool

				// order is not guaranteed ATM
				for j := range labels {
					if labels[j].Name == testCase.labels[i].Name {
						assert.Equal(t, testCase.labels[i].Value, labels[j].Value)
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("Not found label %s: \n"+
						"expected: %v\n"+
						"actual  : %v", testCase.labels[i].Name, testCase.labels, labels))
				}
			}
		})
	}
}
