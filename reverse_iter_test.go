package iradix

import (
	"fmt"
	"testing"
)

func TestReverseIterator_SeekReverseLowerBound(t *testing.T) {
	r := New()
	keys := []string{"001", "002", "005", "010", "200", "201"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	cases := []struct {
		name            string
		prefix          string
		want            string
		wantFromNonRoot string
	}{
		{
			name:   "exact match",
			prefix: "002",
			want:   "002",
		},
		{
			name:   "between leaf nodes",
			prefix: "003",
			want:   "002",
		},
		{
			name:   "between non-leaf nodes",
			prefix: "100",
			want:   "010",
		},
		{
			name:   "outbound low",
			prefix: "/", // the character '/' comes before '0' in ASCII
			want:   "",
		},
		{
			name:            "outbound high",
			prefix:          "300",
			want:            "201",
			wantFromNonRoot: "010",
		},
		{
			name:   "long prefix low",
			prefix: "0010",
			want:   "",
		},
		{
			name:            "long prefix high",
			prefix:          "2010",
			want:            "200",
			wantFromNonRoot: "010",
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("from_root/%s", c.name), func(t *testing.T) {
			it := r.Root().ReverseIterator()
			it.SeekReverseLowerBound([]byte(c.prefix))
			got, _, _ := it.Previous()

			if string(got) != c.want {
				t.Errorf("prefix %s seek failed: got: %s, want: %s", c.prefix, got, c.want)
			}
		})

		t.Run(fmt.Sprintf("from_non_root/%s", c.name), func(t *testing.T) {
			n := r.Root().edges[0].node
			it := n.ReverseIterator()
			it.SeekReverseLowerBound([]byte(c.prefix))
			got, _, _ := it.Previous()

			want := c.wantFromNonRoot
			if want == "" {
				want = c.want
			}

			if string(got) != want {
				t.Errorf("prefix %s seek failed: got: %s, want: %s", c.prefix, got, want)
			}
		})
	}
}

func TestReverseIterator_SeekPrefix(t *testing.T) {
	r := New()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	cases := []struct {
		name         string
		prefix       string
		expectResult bool
	}{
		{
			name:         "existing prefix",
			prefix:       "005",
			expectResult: true,
		},
		{
			name:         "non-existing prefix",
			prefix:       "2",
			expectResult: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := r.Root().ReverseIterator()
			it.SeekPrefix([]byte(c.prefix))

			if c.expectResult && it.i.node == nil {
				t.Errorf("expexted prefix %s to exist", c.prefix)
				return
			}

			if !c.expectResult && it.i.node != nil {
				t.Errorf("unexpected node for prefix '%s'", c.prefix)
				return
			}
		})
	}
}

func TestReverseIterator_Previous(t *testing.T) {
	r := New()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	it := r.Root().ReverseIterator()

	for i := len(keys) - 1; i >= 0; i-- {
		got, _, _ := it.Previous()
		want := keys[i]

		if string(got) != want {
			t.Errorf("got: %v, want: %v", got, want)
		}
	}
}
