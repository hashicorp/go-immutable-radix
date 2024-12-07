// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package iradix

import (
	"bytes"
)

// ReverseIterator is used to iterate over a set of nodes
// in reverse in-order.
type ReverseIterator[T any] struct {
	i *Iterator[T]

	// expandedParents stores parent nodes whose relevant children have
	// already been pushed into the stack.
	expandedParents map[*Node[T]]struct{}
}

// NewReverseIterator returns a new ReverseIterator at a node
func NewReverseIterator[T any](n *Node[T]) *ReverseIterator[T] {
	return &ReverseIterator[T]{
		i: &Iterator[T]{node: n},
	}
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (ri *ReverseIterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	return ri.i.SeekPrefixWatch(prefix)
}

// SeekPrefix is used to seek the iterator to a given prefix
func (ri *ReverseIterator[T]) SeekPrefix(prefix []byte) {
	ri.i.SeekPrefixWatch(prefix)
}

// SeekReverseLowerBound sets the iterator to the largest key <= the given key
func (ri *ReverseIterator[T]) SeekReverseLowerBound(key []byte) {
	// Clear the stack.
	ri.i.stack = nil
	n := ri.i.node
	ri.i.node = nil

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	found := func(n *Node[T]) {
		ri.i.stack = append(ri.i.stack, n)
		ri.expandedParents[n] = struct{}{}
	}

	search := key
	for {
		var prefixCmp int
		if len(n.prefix) < len(search) {
			prefixCmp = bytes.Compare(n.prefix, search[:len(n.prefix)])
		} else {
			prefixCmp = bytes.Compare(n.prefix, search)
		}

		if prefixCmp < 0 {
			// Current node prefix is smaller than search prefix,
			// so we push this node and let the iterator descend to find the max leaf.
			ri.i.stack = append(ri.i.stack, n)
			return
		}

		if prefixCmp > 0 {
			// Current node prefix is larger than the search prefix,
			// no reverse lower bound here.
			return
		}

		// prefixCmp == 0
		if n.isLeaf() {
			if bytes.Equal(n.leaf.key, key) {
				// Exact match
				found(n)
				return
			}

			// Leaf is lower than key and could be lower bound
			if len(n.children) == 0 {
				found(n)
				return
			}

			// Leaf with children, add it now. The iterator will handle children later.
			ri.i.stack = append(ri.i.stack, n)
			ri.expandedParents[n] = struct{}{}
		}

		// Consume the matched prefix
		search = search[len(n.prefix):]

		if len(search) == 0 {
			// Exhausted search key, no more exact match.
			return
		}

		idx, child := n.getLowerBoundEdge(search[0])
		if idx == -1 {
			idx = len(n.children)
		}

		// Children before idx are strictly lower than search
		for _, cnode := range n.children[:idx] {
			ri.i.stack = append(ri.i.stack, cnode)
		}

		if child == nil {
			// No lower bound child, done.
			return
		}

		n = child
	}
}

// Previous returns the previous node in reverse order
func (ri *ReverseIterator[T]) Previous() ([]byte, T, bool) {
	var zero T
	// Initialize stack if needed
	if ri.i.stack == nil && ri.i.node != nil {
		ri.i.stack = append(ri.i.stack, ri.i.node)
	}

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	for len(ri.i.stack) > 0 {
		// Pop the top node
		elem := ri.i.stack[len(ri.i.stack)-1]
		ri.i.stack = ri.i.stack[:len(ri.i.stack)-1]

		_, alreadyExpanded := ri.expandedParents[elem]

		// If this node has children and not expanded, we must expand it now.
		if len(elem.children) > 0 && !alreadyExpanded {
			ri.expandedParents[elem] = struct{}{}
			// We want to visit children (which are greater) first.
			// Push children onto the stack so that the largest child is visited first.
			// Since we pop from the end, we push children in ascending order so that
			// the last pushed (largest) is popped first.
			for _, child := range elem.children {
				ri.i.stack = append(ri.i.stack, child)
			}

			// Also push this node back so that after children are visited,
			// we can return its leaf if present.
			ri.i.stack = append(ri.i.stack, elem)
			continue
		}

		// If we had expanded this node before, remove it from expandedParents
		if alreadyExpanded {
			delete(ri.expandedParents, elem)
		}

		// If this node has a leaf, return it
		if elem.leaf != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
		// Otherwise, keep going
	}

	return nil, zero, false
}
