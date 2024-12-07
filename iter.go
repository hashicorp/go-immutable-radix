// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package iradix

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes
// in pre-order
type Iterator[T any] struct {
	node  *Node[T]
	stack []*Node[T]
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (i *Iterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Wipe the stack
	i.stack = nil
	n := i.node
	watch = n.mutateCh
	search := prefix
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			i.node = n
			return
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			i.node = nil
			return
		}

		// Update to the finest granularity as the search makes progress
		watch = n.mutateCh

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			i.node = n
			return
		} else {
			i.node = nil
			return
		}
	}
}

// SeekPrefix is used to seek the iterator to a given prefix
func (i *Iterator[T]) SeekPrefix(prefix []byte) {
	i.SeekPrefixWatch(prefix)
}

func (i *Iterator[T]) recurseMin(n *Node[T]) *Node[T] {
	// Traverse to the minimum child
	if n.leaf != nil {
		return n
	}

	nChildren := len(n.children)
	if nChildren > 1 {
		// Add all the other children to the stack (the min node will be added as
		// we recurse down the first child)
		i.stack = append(i.stack, n.children[1:]...)
	}

	if nChildren > 0 {
		return i.recurseMin(n.children[0])
	}

	// Shouldn't be possible if the tree is well-formed
	return nil
}

// SeekLowerBound is used to seek the iterator to the smallest key that is
// greater or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (i *Iterator[T]) SeekLowerBound(key []byte) {
	// Wipe the stack. Unlike Prefix iteration, we need to build the stack as we
	// go because we need only a subset of edges of many nodes in the path to the
	// leaf with the lower bound. Note that the iterator will still recurse into
	// children that we don't traverse on the way to the reverse lower bound as it
	// walks the stack.
	i.stack = []*Node[T]{}
	// i.node starts off in the common case as pointing to the root node of the
	// tree. By the time we return we have either found a lower bound and setup
	// the stack to traverse all larger keys, or we have not and the stack and
	// node should both be nil to prevent the iterator from assuming it is just
	// iterating the whole tree from the root node. Either way this needs to end
	// up as nil so just set it here.
	n := i.node
	i.node = nil
	search := key

	found := func(n *Node[T]) {
		i.stack = append(
			i.stack,
			n,
		)
	}

	findMin := func(n *Node[T]) {
		n = i.recurseMin(n)
		if n != nil {
			found(n)
			return
		}
	}

	for {
		// Compare current prefix with the search key's same-length prefix.
		var prefixCmp int
		if len(n.prefix) < len(search) {
			prefixCmp = bytes.Compare(n.prefix, search[0:len(n.prefix)])
		} else {
			prefixCmp = bytes.Compare(n.prefix, search)
		}

		if prefixCmp > 0 {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			findMin(n)
			return
		}

		if prefixCmp < 0 {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			return
		}

		// Prefix is equal, we are still heading for an exact match. If this is a
		// leaf and an exact match we're done.
		if n.leaf != nil && bytes.Equal(n.leaf.key, key) {
			found(n)
			return
		}

		// Consume the search prefix if the current node has one. Note that this is
		// safe because if n.prefix is longer than the search slice prefixCmp would
		// have been > 0 above and the method would have already returned.
		search = search[len(n.prefix):]

		if len(search) == 0 {
			// We've exhausted the search key, but the current node is not an exact
			// match or not a leaf. That means that the leaf value if it exists, and
			// all child nodes must be strictly greater, the smallest key in this
			// subtree must be the lower bound.
			findMin(n)
			return
		}

		// Otherwise, take the lower bound next edge.
		idx, lbNode := n.getLowerBoundEdge(search[0])
		if lbNode == nil {
			return
		}

		// Create stack edges for the all strictly higher edges in this node.
		if idx+1 < len(n.children) {
			i.stack = append(i.stack, n.children[idx+1:]...)
		}

		// Recurse
		n = lbNode
	}
}

func (i *Iterator[T]) Next() ([]byte, T, bool) {
	var zero T

	// Initialize stack if needed
	if i.stack == nil && i.node != nil {
		i.stack = []*Node[T]{i.node}
	}

	for len(i.stack) > 0 {
		// Pop the top node
		n := i.stack[len(i.stack)-1]
		i.stack = i.stack[:len(i.stack)-1]

		// Push children in reverse order, so the leftmost child
		// is visited next, maintaining a pre-order traversal.
		for c := len(n.children) - 1; c >= 0; c-- {
			i.stack = append(i.stack, n.children[c])
		}

		// Return the leaf values if any
		if n.leaf != nil {
			return n.leaf.key, n.leaf.val, true
		}
	}

	return nil, zero, false
}
