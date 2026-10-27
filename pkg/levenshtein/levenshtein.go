package levenshtein

import (
	"bytes"
)

// newrun creates a run for computing
// distances to input string b.
func newrun(b []byte) *run {
	br := []rune(string(b))
	return &run{
		b: br,
		d: make([]int, len(br)+1),
	}
}

type run struct {
	b []rune // should be the larger of the two comparisons
	d []int
}

// dist computes the Levenshtein distance from a to the runner string.
func (l *run) dist(a []byte) int {
	d := l.d
	b := l.b

	for j := range d {
		d[j] = j
	}

	for _, ca := range string(a) {
		j := 1
		dj1 := d[0]
		d[0]++
		for _, cb := range b {
			mn := min(d[j]+1, d[j-1]+1) // delete & insert
			if cb != ca {
				mn = min(mn, dj1+1) // change
			} else {
				mn = min(mn, dj1) // matched
			}

			dj1, d[j] = d[j], mn
			j++
		}
	}

	return d[len(d)-1]
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// HasPrefix is analogous to bytes.HasPrefix except the prefix
// can fuzzy match based on the Levenshtein distance threshold
// between s and prefix
func HasPrefix(s, prefix []byte, threshold int) (int, bool) {
	l := len(prefix)
	if l > len(s) {
		return 0, false
	}
	if len(s) > l {
		s = s[:l]
	}
	if bytes.Equal(s, prefix) {
		return 0, true
	}
	r := newrun(s)
	d := r.dist(prefix)

	return d, d <= threshold
}
