package iradix

import (
	"sort"
	"testing"
	"testing/quick"
)

func TestReverseIterator_SeekReverseLowerBoundFuzz(t *testing.T) {
	r := New()
	set := []string{}

	// This specifies a property where each call adds a new random key to the radix
	// tree (with a null byte appended since our tree doesn't support one key
	// being a prefix of another and treats null bytes specially).
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the beginning using ReverseLowerBound
	// produces the same list as filtering all sorted keys that are bigger.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		// Append a null byte
		key := []byte(newKey + "\x00")
		r, _, _ = r.Insert(key, nil)

		// Now iterate the tree from searchKey to the beginning
		it := r.Root().ReverseIterator()
		result := []string{}
		it.SeekReverseLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Previous()
			if !ok {
				break
			}
			// Strip the null byte and append to result set
			result = append(result, string(key[0:len(key)-1]))
		}
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		result := []string{}
		var prev string
		for i := len(set) - 1; i >= 0; i-- {
			k := set[i]
			if k <= string(searchKey) && k != prev {
				result = append(result, k)
			}
			prev = k
		}
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, nil); err != nil {
		t.Error(err)
	}
}

func TestReverseIterator_SeekReverseLowerBoundFuzzFromNonRoot(t *testing.T) {
	// Some edge cases are only triggered when seeking from a non-root node,
	// such as when looking for a key that is larger than the values currently
	// in the tree.
	//
	// When starting from the root, the prefix is empty and so it will always
	// match the subset of the search key of same length (they are both empty).
	// The search for the lower bound will then return nil (all keys in the
	// tree are lower than the search key) and the seek process is cut short.
	//
	// But when starting from a non-root node, the prefix is not empty and so
	// it will require a recursive search for the glabal maximum in the
	// sub-tree, which is not needed when starting from the root.

	r := New()
	set := []string{}
	var n *Node

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		// Append a null byte
		key := []byte(newKey + "\x00")
		r, _, _ = r.Insert(key, nil)

		// Start seeking from the first root child or don't seek yet
		if len(r.Root().edges) == 0 {
			return []string{}
		}
		n = r.Root().edges[0].node

		// Now iterate the tree from searchKey to the beginning
		it := n.ReverseIterator()
		result := []string{}
		it.SeekReverseLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Previous()
			if !ok {
				break
			}
			// Strip the null byte and append to result set
			result = append(result, string(key[0:len(key)-1]))
		}
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Return if the tree doesn't have a non-root node present yet
		if n == nil {
			return []string{}
		}

		// Remove the null byte from the prefix if present
		prefix := n.prefix
		if prefix[len(prefix)-1] == byte('\x00') {
			prefix = prefix[:len(prefix)-1]
		}

		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		result := []string{}
		var prev string
		for i := len(set) - 1; i >= 0; i-- {
			k := set[i]
			if k <= string(searchKey) && k[:len(prefix)] <= string(prefix) && k != prev {
				result = append(result, k)
			}
			prev = k
		}
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, nil); err != nil {
		t.Error(err)
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

func TestReverseIterator_SeekPrefixWatch(t *testing.T) {
	key := []byte("key")

	// Create tree
	r := New()
	r, _, _ = r.Insert(key, nil)

	// Find mutate channel
	it := r.Root().ReverseIterator()
	ch := it.SeekPrefixWatch(key)

	// Change prefix
	tx := r.Txn()
	tx.TrackMutate(true)
	tx.Insert(key, "value")
	tx.Commit()

	// Check if channel closed
	select {
	case <-ch:
	default:
		t.Errorf("channel not closed")
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
