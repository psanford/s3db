package etag

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`"abc123"`, "abc123"},
		{`abc123`, "abc123"},
		{`""`, ""},
		{``, ""},
		{`"a"b"`, `a"b`}, // only strips outer quotes
	}
	for _, tc := range cases {
		if got := Normalize(tc.in); got != tc.want {
			t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCompute(t *testing.T) {
	// Known MD5 values.
	cases := []struct {
		body []byte
		want string
	}{
		{[]byte(""), "d41d8cd98f00b204e9800998ecf8427e"},
		{[]byte("hello"), "5d41402abc4b2a76b9719d911017c592"},
	}
	for _, tc := range cases {
		if got := Compute(tc.body); got != tc.want {
			t.Errorf("Compute(%q) = %q, want %q", tc.body, got, tc.want)
		}
	}
}

func TestCompute_Deterministic(t *testing.T) {
	a := Compute([]byte("same"))
	b := Compute([]byte("same"))
	if a != b {
		t.Errorf("Compute not deterministic: %q vs %q", a, b)
	}
}

func TestCompute_DifferentInputs(t *testing.T) {
	a := Compute([]byte("foo"))
	b := Compute([]byte("bar"))
	if a == b {
		t.Error("different inputs produced same etag")
	}
}
