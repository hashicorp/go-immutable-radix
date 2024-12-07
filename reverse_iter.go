package iradix

import (
	"bytes"
)

// ReverseIterator is used to iterate over a set of nodes in reverse in-order.
type ReverseIterator[T any] struct {
	i *Iterator[T]

	// expandedParents stores parent nodes whose relevant children have
	// already been pushed onto the stack.
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

// SeekReverseLowerBound sets the iterator to the largest key <= key.
func (ri *ReverseIterator[T]) SeekReverseLowerBound(key []byte) {
	// Clear the stack.
	ri.i.stack = nil
	n := ri.i.node
	ri.i.node = nil
	search := key

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	// found adds a single node as a slice to the stack and marks it as expanded.
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
			// Current prefix < search prefix.
			// For reverse lower bound, we want the largest leaf under this subtree.
			// Push this node and let the iterator's Previous handle descending.
			ri.i.stack = append(ri.i.stack, []*Node[T]{n})
			return
		}

		if prefixCmp > 0 {
			// Current prefix > search prefix: no reverse lower bound here.
			return
		}

		// prefixCmp == 0
		if n.isLeaf() {
			if bytes.Equal(n.leaf.key, key) {
				// Exact match
				found(n)
				return
			}

			// Leaf is lower than key.
			if len(n.children) == 0 {
				// This leaf is the lower bound.
				found(n)
				return
			}

			// Leaf with children.
			// Push this node so we consider its leaf first.
			ri.i.stack = append(ri.i.stack, []*Node[T]{n})
			ri.expandedParents[n] = struct{}{}
		}

		// Consume matched prefix.
		search = search[len(n.prefix):]

		if len(search) == 0 {
			// Exhausted the search key but not at a leaf.
			// All children are greater than search, so no reverse lower bound here.
			return
		}

		// Find the lower bound child.
		idx, lbNode := n.getLowerBoundEdge(search[0])
		if idx == -1 {
			idx = len(n.children)
		}

		// Children before idx are strictly lower than search.
		if idx > 0 {
			ri.i.stack = append(ri.i.stack, n.children[:idx])
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
	// Initialize stack if needed.
	if ri.i.stack == nil && ri.i.node != nil {
		ri.i.stack = append(ri.i.stack, []*Node[T]{ri.i.node})
	}

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[*Node[T]]struct{})
	}

	for len(ri.i.stack) > 0 {
		n := len(ri.i.stack)
		last := ri.i.stack[n-1]
		m := len(last)
		elem := last[m-1]

		_, alreadyExpanded := ri.expandedParents[elem]

		// If this node has children and not expanded, we must expand now.
		// Reverse order: we want largest children first.
		if len(elem.children) > 0 && !alreadyExpanded {
			ri.expandedParents[elem] = struct{}{}

			// Push children as a slice. For reverse iteration, the largest child
			// should be visited first, so we rely on popping from the end.
			// Because we pop from the end, the last child in `elem.children` is the largest.
			// No need to reverse here if we assume children are in ascending order.
			ri.i.stack = append(ri.i.stack, elem.children)

			// Also push `elem` back so after children, we consider its leaf.
			ri.i.stack = append(ri.i.stack, []*Node[T]{elem})
			// Continue to process after expansion.
			// We don't remove `elem` now since we re-added it after children.
			// Next iteration will handle the children and then come back to elem.
			ri.i.stack = ri.i.stack[:n] // Remove the original slice from the stack end
			continue
		}

		// Remove the node from the current slice.
		if m > 1 {
			ri.i.stack[n-1] = last[:m-1]
		} else {
			ri.i.stack = ri.i.stack[:n-1]
		}

		if alreadyExpanded {
			delete(ri.expandedParents, elem)
		}

		// If this node has a leaf, return it.
		if elem.leaf != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
		// Otherwise, continue until we find a leaf.
	}

	return nil, zero, false
}
