package engine

import "testing"

func TestWSURLFromHTTP(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"https://matching-engine-w45t.onrender.com", "wss://matching-engine-w45t.onrender.com/ws"},
		{"http://localhost:8080", "ws://localhost:8080/ws"},
		{"https://matching-engine-w45t.onrender.com/", "wss://matching-engine-w45t.onrender.com/ws"},
	}
	for _, c := range cases {
		got, err := wsURLFromHTTP(c.in)
		if err != nil {
			t.Fatalf("wsURLFromHTTP(%q) error: %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("wsURLFromHTTP(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
