package iradix

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"testing/quick"
)

func TestReverseIterator_SeekReverseLowerBoundFuzz(t *testing.T) {
	r := New()
	set := []string{}

	// This specifies a property where each call adds a new random key to the radix
	// tree.
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the beginning using ReverseLowerBound
	// produces the same list as filtering all sorted keys that are bigger.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		r, _, _ = r.Insert([]byte(newKey), nil)

		// Now iterate the tree from searchKey to the beginning
		it := r.Root().ReverseIterator()
		result := []string{}
		it.SeekReverseLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Previous()
			if !ok {
				break
			}
			result = append(result, string(key))
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
	// it will require a recursive search for the global maximum in the
	// sub-tree, which is not needed when starting from the root.

	r := New()
	set := []string{}
	var n *Node

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		r, _, _ = r.Insert([]byte(newKey), nil)

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
			result = append(result, string(key))
		}
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Return if the tree doesn't have a non-root node present yet
		if n == nil {
			return []string{}
		}

		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		result := []string{}
		var prev string
		for i := len(set) - 1; i >= 0; i-- {
			k := set[i]
			if k <= string(searchKey) && k[:len(n.prefix)] <= string(n.prefix) && k != prev {
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

func TestReverseIterator_SeekLowerBound(t *testing.T) {

	var revFixedLenKeys, revMixedLenKeys []string

	for i := len(fixedLenKeys) - 1; i >= 0; i-- {
		revFixedLenKeys = append(revFixedLenKeys, fixedLenKeys[i])
	}
	for i := len(mixedLenKeys) - 1; i >= 0; i-- {
		revMixedLenKeys = append(revMixedLenKeys, mixedLenKeys[i])
	}

	type exp struct {
		keys   []string
		search string
		want   []string
	}
	cases := []exp{
		{
			fixedLenKeys,
			"20020",
			revFixedLenKeys,
		},
		{
			fixedLenKeys,
			"20000",
			[]string{
				"00020",
				"00010",
				"00004",
				"00001",
				"00000",
			},
		},
		{
			fixedLenKeys,
			"00010",
			[]string{
				"00010",
				"00004",
				"00001",
				"00000",
			},
		},
		{
			fixedLenKeys,
			"00000",
			[]string{
				"00000",
			},
		},
		{
			fixedLenKeys,
			"0",
			[]string{},
		},
		{
			mixedLenKeys,
			"{", // after all lower case letters
			revMixedLenKeys,
		},
		{
			mixedLenKeys,
			"zip",
			revMixedLenKeys,
		},
		{
			mixedLenKeys,
			"b",
			[]string{
				"abc",
				"a1",
			},
		},
		{
			mixedLenKeys,
			"barbazboo0",
			[]string{
				"barbazboo",
				"abc",
				"a1",
			},
		},
		{
			mixedLenKeys,
			"a",
			[]string{},
		},
		{
			mixedLenKeys,
			"a1",
			[]string{
				"a1",
			},
		},

		// We SHOULD support keys that are prefixes of each other despite some
		// confusion in the original implementation.
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foo",
			[]string{"foo", "fo", "f", "bug"},
		},
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foozzzzzzzzzz", // larger than any key but with shared prefix
			[]string{"food", "foo", "fo", "f", "bug"},
		},

		// We also support the empty key (which is a prefix of every other key) as a
		// valid key to insert and search for.
		{
			[]string{"f", "fo", "foo", "food", "bug", ""},
			"foo",
			[]string{"foo", "fo", "f", "bug", ""},
		},
		{
			[]string{"f", "bug", ""},
			"",
			[]string{""},
		},
		{
			[]string{"f", "bug", "xylophone"},
			"",
			[]string{},
		},

		// This case could panic before. it involves a node with a shared prefix and
		// children where the reverse lower bound is greater than all the children
		{
			[]string{"foo00", "foo11"},
			"foo",
			[]string{},
		},

		// When fixing the panic above the above test could pass but we need to
		// verify the logic is still correct in the case there was a lower bound in
		// another node.
		{
			[]string{"bar", "foo00", "foo11"},
			"foo",
			[]string{"bar"},
		},
	}

	for idx, test := range cases {
		t.Run(fmt.Sprintf("case%03d", idx), func(t *testing.T) {
			r := New()

			// Insert keys
			for _, k := range test.keys {
				var ok bool
				r, _, ok = r.Insert([]byte(k), nil)
				if ok {
					t.Fatalf("duplicate key %s in keys", k)
				}
			}
			if r.Len() != len(test.keys) {
				t.Fatal("failed adding keys")
			}
			// Get and seek iterator
			root := r.Root()
			iter := root.ReverseIterator()
			iter.SeekReverseLowerBound([]byte(test.search))

			// Consume all the keys
			out := []string{}
			for {
				key, _, ok := iter.Previous()
				if !ok {
					break
				}
				out = append(out, string(key))
			}
			if !reflect.DeepEqual(out, test.want) {
				t.Fatalf("mis-match: key=%s\n  got=%v\n  want=%v", test.search,
					out, test.want)
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
