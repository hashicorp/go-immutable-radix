package iradix

import (
	"bytes"
)

// ReverseIterator is used to iterate over a set of nodes in reverse in-order.
type ReverseIterator[T any] struct {
	i *Iterator[T]

	// expandedParents keeps track of nodes whose edges have been pushed.
	expandedParents map[*Node[T]]struct{}
}

// NewReverseIterator returns a new ReverseIterator at a node
func NewReverseIterator[T any](n *Node[T]) *ReverseIterator[T] {
	return &ReverseIterator[T]{
		i: &Iterator[T]{node: n},
	}
}

// SeekPrefixWatch seeks the iterator to a given prefix and returns the watch channel.
func (ri *ReverseIterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	return ri.i.SeekPrefixWatch(prefix)
}

// SeekPrefix seeks the iterator to a given prefix.
func (ri *ReverseIterator[T]) SeekPrefix(prefix []byte) {
	ri.i.SeekPrefixWatch(prefix)
}

// SeekReverseLowerBound sets the iterator to the largest key <= 'key'.
func (ri *ReverseIterator[T]) SeekReverseLowerBound(key []byte) {
	ri.i.stack = nil
	n := ri.i.node
	ri.i.node = nil
	search := key

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	// found adds a single node as a slice and marks it as expanded
	found := func(n *Node[T]) {
		ri.i.stack = append(ri.i.stack, []*Node[T]{n})
		ri.expandedParents[n] = struct{}{}
	}

	for {
		var prefixCmp int
		if len(n.prefix) < len(search) {
			prefixCmp = bytes.Compare(n.prefix, search[:len(n.prefix)])
		} else {
			prefixCmp = bytes.Compare(n.prefix, search)
		}

		if prefixCmp < 0 {
			// n.prefix < search => reverse lower bound is under this subtree.
			// Push this node; the reverse iteration (Previous) will descend into it.
			ri.i.stack = append(ri.i.stack, []*Node[T]{n})
			return
		}

		if prefixCmp > 0 {
			// n.prefix > search => no reverse lower bound here.
			return
		}

		// prefixCmp == 0
		if n.isLeaf() {
			if bytes.Equal(n.leaf.key, key) {
				// Exact match
				found(n)
				return
			}

			// Leaf < key (since not equal). If no edges, this leaf is the lower bound.
			if len(n.edges) == 0 {
				found(n)
				return
			}

			// Leaf with edges. Push node first, mark expanded.
			ri.i.stack = append(ri.i.stack, []*Node[T]{n})
			ri.expandedParents[n] = struct{}{}
		}

		// Consume matched prefix
		search = search[len(n.prefix):]

		if len(search) == 0 {
			// Exhausted search key, not at a leaf, all edges > search => no lower bound here.
			return
		}

		idx, lbNode := n.getLowerBoundEdge(search[0])
		if idx == -1 {
			idx = len(n.edges)
		}

		// Children before idx are strictly lower than search
		if idx > 0 {
			ri.i.stack = append(ri.i.stack, n.edges[:idx])
		}

		if lbNode == nil {
			// No lower bound child
			return
		}

		n = lbNode
	}
}

// Previous returns the previous node in reverse order.
func (ri *ReverseIterator[T]) Previous() ([]byte, T, bool) {
	var zero T
	if ri.i.stack == nil && ri.i.node != nil {
		// Initialize stack with the root node if needed
		ri.i.stack = append(ri.i.stack, []*Node[T]{ri.i.node})
	}

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	for len(ri.i.stack) > 0 {
		// Get the top slice of nodes
		n := len(ri.i.stack)
		top := ri.i.stack[n-1]
		m := len(top)
		elem := top[m-1] // The top node on the stack

		// Pop this node from the top slice
		if m > 1 {
			ri.i.stack[n-1] = top[:m-1]
		} else {
			ri.i.stack = ri.i.stack[:n-1]
		}

		_, alreadyExpanded := ri.expandedParents[elem]

		// If this node has edges and isn't expanded, expand now.
		if len(elem.edges) > 0 && !alreadyExpanded {
			ri.expandedParents[elem] = struct{}{}

			// After processing edges, we want to revisit this node (elem).
			// Push it back as a single-node slice, so its leaf is considered after its edges.
			ri.i.stack = append(ri.i.stack, []*Node[T]{elem})

			// For reverse order, we want to visit the largest child first.
			// By default, edges are in ascending order. We rely on popping last element first,
			// so we can append edges as is. The last child in edges is largest.
			ri.i.stack = append(ri.i.stack, elem.edges)

			continue
		}

		// If already expanded or no edges, we've fully popped elem now.
		if alreadyExpanded {
			delete(ri.expandedParents, elem)
		}

		// If elem has a leaf, return it
		if elem.leaf != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
		// If no leaf, continue
	}

	return nil, zero, false
}
