package iradix

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeWalk(t *testing.T) {
	r := New[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	i := 0

	r.Root().Walk(func(k []byte, _ any) bool {
		got := string(k)
		want := keys[i]
		if got != want {
			t.Errorf("got %s, want: %s", got, want)
		}

		i++
		if i >= len(keys) {
			return true
		}

		return false
	})
}

func TestNodeWalkBackwards(t *testing.T) {
	r := New[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	i := len(keys) - 1

	r.Root().WalkBackwards(func(k []byte, _ any) bool {
		got := string(k)
		want := keys[i]
		if got != want {
			t.Errorf("got %s, want: %s", got, want)
		}

		i--
		if i < 0 {
			return true
		}

		return false
	})
}

// loadLines loads the lines from a file in the testdata directory.
func loadLines(tb testing.TB, filename string) []string {
	tb.Helper()

	f, err := os.Open(filepath.Join("testdata", filename))
	require.NoError(tb, err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	require.NoError(tb, scanner.Err())

	return lines
}

type benchTest struct {
	name     string
	prefix   string
	expected string
}

var longestPrefixTests = []struct {
	filename string
	tests    []benchTest
}{
	{
		filename: "words",
		tests: []benchTest{
			{name: "exact-match-early", prefix: "ageless", expected: "ageless"},
			{name: "exact-match-late", prefix: "Wachenheimer", expected: "Wachenheimer"},
			{name: "prefix-match-early", prefix: "archconspirator+other", expected: "archconspirator"},
			{name: "prefix-match-late", prefix: "ureametry+other", expected: "ureametry"},
			{name: "not-found-early", prefix: "0000-notfound"},
			{name: "not-found-late", prefix: "~xxx-notfound"},
		},
	},
	{
		filename: "uuids",
		tests: []benchTest{
			{name: "exact-match-early", prefix: "85f9f198-6bae-4c57-a61d-ee31be8dcec3", expected: "85f9f198-6bae-4c57-a61d-ee31be8dcec3"},
			{name: "exact-match-late", prefix: "b938fb33-9ed7-4658-9b88-faa82443645c", expected: "b938fb33-9ed7-4658-9b88-faa82443645c"},
			{name: "prefix-match-early", prefix: "fcb5e951-feed-4a54-98fb-281b6a3f175c+other", expected: "fcb5e951-feed-4a54-98fb-281b6a3f175c"},
			{name: "prefix-match-late", prefix: "a17b956c-438c-4932-aff8-7e66093022d8+something", expected: "a17b956c-438c-4932-aff8-7e66093022d8"},
			{name: "not-found-early", prefix: "0000-notfound"},
			{name: "not-found-late", prefix: "~xxx-notfound"},
		},
	},
	{
		filename: "endpoints",
		tests: []benchTest{
			{name: "exact-match-early", prefix: "/applications/{client_id}/token/scoped", expected: "/applications/{client_id}/token/scoped"},
			{name: "exact-match-late", prefix: "/users/{username}/packages/{package_type}/{package_name}", expected: "/users/{username}/packages/{package_type}/{package_name}"},
			{name: "prefix-match-early", prefix: "/gists/{gist_id}+other", expected: "/gists/{gist_id}"},
			{name: "prefix-match-late", prefix: "/user/installations+other", expected: "/user/installations"},
			{name: "not-found-early", prefix: "0000-notfound"},
			{name: "not-found-late", prefix: "~xxx-notfound"},
		},
	},
}

func TestLongestPrefixLong(t *testing.T) {
	for _, tc := range longestPrefixTests {
		for _, test := range tc.tests {
			t.Run(tc.filename+"-"+test.name, func(t *testing.T) {
				lines := loadLines(t, tc.filename+".txt")
				r := New[struct{}]()
				txn := r.Txn()
				for _, l := range lines {
					txn.Insert([]byte(l), struct{}{})
				}

				root := txn.Commit().Root()

				got, _, _ := root.LongestPrefix([]byte(test.prefix))
				require.Equal(t, test.expected, string(got))
			})
		}
	}
}

func BenchmarkLongestPrefix(b *testing.B) {
	for _, tc := range longestPrefixTests {
		for _, test := range tc.tests {
			b.Run(tc.filename+"-"+test.name, func(b *testing.B) {
				lines := loadLines(b, tc.filename+".txt")
				r := New[struct{}]()
				txn := r.Txn()
				for _, l := range lines {
					txn.Insert([]byte(l), struct{}{})
				}

				root := txn.Commit().Root()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					root.LongestPrefix([]byte(test.prefix))
				}
			})
		}
	}
}
