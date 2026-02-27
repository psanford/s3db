package main

import "testing"

func TestParseS3URL(t *testing.T) {
	cases := []struct {
		in         string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{"s3://bucket/prefix/", "bucket", "prefix/", false},
		{"s3://bucket/prefix", "bucket", "prefix/", false}, // trailing / added
		{"s3://bucket/nested/deep/prefix/", "bucket", "nested/deep/prefix/", false},
		{"s3://bucket/", "", "", true}, // bucket-root rejected
		{"s3://bucket", "", "", true},  // bucket-root rejected
		{"s3://bucket/db-name/", "bucket", "db-name/", false},
		{"s3:///prefix/", "", "", true}, // empty bucket
		{"http://bucket/prefix/", "", "", true},
		{"bucket/prefix/", "", "", true},
		{"", "", "", true},
	}

	for _, tc := range cases {
		b, p, err := parseS3URL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseS3URL(%q): expected error, got bucket=%q prefix=%q", tc.in, b, p)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseS3URL(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if b != tc.wantBucket {
			t.Errorf("parseS3URL(%q): bucket = %q, want %q", tc.in, b, tc.wantBucket)
		}
		if p != tc.wantPrefix {
			t.Errorf("parseS3URL(%q): prefix = %q, want %q", tc.in, p, tc.wantPrefix)
		}
	}
}
