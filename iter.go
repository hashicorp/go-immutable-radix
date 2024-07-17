package iradix

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes
// in pre-order
type Iterator struct {
	node               *Node
	stack              []edges
	leafNode           *LeafNode
	key                []byte
	seekLowerBound     bool
	seekLowerBoundFlag bool
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (i *Iterator) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Wipe the stack
	i.stack = nil
	i.key = prefix
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
func (i *Iterator) SeekPrefix(prefix []byte) {
	i.SeekPrefixWatch(prefix)
}

func (i *Iterator) recurseMin(n *Node) *Node {
	// Traverse to the minimum child
	if n.leaf != nil {
		return n
	}
	nEdges := len(n.edges)
	if nEdges > 1 {
		// Add all the other edges to the stack (the min node will be added as
		// we recurse)
		i.stack = append(i.stack, n.edges[1:])
	}
	if nEdges > 0 {
		return i.recurseMin(n.edges[0].node)
	}
	// Shouldn't be possible
	return nil
}

// SeekLowerBound is used to seek the iterator to the smallest key that is
// greater or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (i *Iterator) SeekLowerBound(key []byte) {
	i.seekLowerBound = true
	// Wipe the stack. Unlike Prefix iteration, we need to build the stack as we
	// go because we need only a subset of edges of many nodes in the path to the
	// leaf with the lower bound. Note that the iterator will still recurse into
	// children that we don't traverse on the way to the reverse lower bound as it
	// walks the stack.
	i.stack = []edges{}
	// i.node starts off in the common case as pointing to the root node of the
	// tree. By the time we return we have either found a lower bound and setup
	// the stack to traverse all larger keys, or we have not and the stack and
	// node should both be nil to prevent the iterator from assuming it is just
	// iterating the whole tree from the root node. Either way this needs to end
	// up as nil so just set it here.
	n := i.node
	i.key = key
	search := key

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
			i.node = n
			return
		}

		if prefixCmp < 0 {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			return
		}

		// Prefix is equal, we are still heading for an exact match. If this is a
		// leaf and an exact match we're done.
		if n.leaf != nil && bytes.Equal(n.leaf.key, key) {
			i.node = n
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
			i.node = n
			return
		}

		i.node = n
		// Otherwise, take the lower bound next edge.
		_, lbNode := n.getLowerBoundEdge(search[0])
		if lbNode == nil {
			return
		}
		// Recurse
		n = lbNode
	}
}

// Next returns the next node in order
func (i *Iterator) Next() ([]byte, interface{}, bool) {

	var zero interface{}

	if i.node != nil && i.leafNode == nil {
		i.leafNode, _ = i.node.MinimumLeaf()
	}

	if i.seekLowerBound {
		for i.leafNode != nil {
			if i.seekLowerBoundFlag || bytes.Compare(i.leafNode.key, i.key) >= 0 {
				i.seekLowerBoundFlag = true
				res := i.leafNode
				i.leafNode = i.leafNode.getNextLeaf()
				if i.leafNode == nil {
					i.node = nil
				}
				return res.key, res.val, true
			} else {
				i.leafNode = i.leafNode.getNextLeaf()
				if i.leafNode == nil {
					i.node = nil
				}
			}
		}

		i.leafNode = nil
		i.node = nil

		return nil, zero, false
	}

	if i.leafNode != nil && bytes.HasPrefix(i.leafNode.key, i.key) {
		res := i.leafNode
		i.leafNode = i.leafNode.getNextLeaf()
		if i.leafNode == nil {
			i.node = nil
		}
		return res.key, res.val, true
	}

	i.leafNode = nil
	i.node = nil

	return nil, zero, false
}
