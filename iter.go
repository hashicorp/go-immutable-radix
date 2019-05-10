package iradix

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes
// in pre-order
type Iterator struct {
	node  *Node
	stack []edges
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (i *Iterator) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Wipe the stack
	i.stack = nil
	n := i.node
	watch = n.mutateCh
	search := prefix
	for {
		// Check for key exhaution
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
func (i *Iterator) SeekPrefix(prefix []byte) {
	i.SeekPrefixWatch(prefix)
}

// SeekLowerBound is used to seek the iterator to the smallest key that is
// greater or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (i *Iterator) SeekLowerBound(key []byte) {
	// Wipe the stack. Unlike Prefix iteration, we need to build the stack as we
	// go because we need only a subset of edges of many nodes in the path to the
	// leaf with the lower bound.
	i.stack = []edges{}
	n := i.node
	search := key

	found := func(n *Node) {
		i.node = n
		i.stack = append(i.stack, edges{edge{node: n}})
	}

	for {
		// Consume the search prefix
		if len(n.prefix) > len(search) {
			search = []byte{}
		} else {
			search = search[len(n.prefix):]
		}

		// Is this a leaf? If so check if it's lower than the key (no lower bound
		// exists).
		if n.leaf != nil {
			if bytes.Compare(n.leaf.key, key) < 0 {
				i.node = nil
				return
			}

			// We are a leaf that is greater or equal, that means we are the lower
			// bound.
			found(n)
			return
		}

		// Check for key exhaustion
		if len(search) == 0 {
			found(n)
			return
		}

		// Not a leaf, look for the lower bound edge
		idx, lbNode := n.getLowerBoundEdge(search[0])
		if lbNode == nil {
			i.node = nil
			return
		}

		// Create stack edges for the all strictly higher edges in this node.
		if idx+1 < len(n.edges) {
			i.stack = append(i.stack, n.edges[idx+1:])
		}

		i.node = lbNode
		// Recurse
		n = lbNode
	}
}

// Next returns the next node in order
func (i *Iterator) Next() ([]byte, interface{}, bool) {
	// Initialize our stack if needed
	if i.stack == nil && i.node != nil {
		i.stack = []edges{
			edges{
				edge{node: i.node},
			},
		}
	}

	for len(i.stack) > 0 {
		// Inspect the last element of the stack
		n := len(i.stack)
		last := i.stack[n-1]
		elem := last[0].node

		// Update the stack
		if len(last) > 1 {
			i.stack[n-1] = last[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		// Push the edges onto the frontier
		if len(elem.edges) > 0 {
			i.stack = append(i.stack, elem.edges)
		}

		// Return the leaf values if any
		if elem.leaf != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
	}
	return nil, nil, false
}
