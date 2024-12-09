// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package iradix

import (
	"bytes"
	"math/bits"
)

// WalkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type WalkFn[T any] func(k []byte, v T) bool

// leafNode is used to represent a value
type leafNode[T any] struct {
	mutateCh chan struct{}
	key      []byte
	val      T
}

// edge is used to represent an edge node
type edge[T any] struct {
	label byte
	node  *Node[T]
}

// Node is an immutable node in the radix tree
type Node[T any] struct {
	// mutateCh is closed if this node is modified
	mutateCh chan struct{}

	// leaf is used to store possible leaf
	leaf *leafNode[T]

	// prefix is the common prefix we ignore
	prefix []byte

	// bitmap represents which edges exist.
	// There are 256 possible edges (one per byte),
	// so we use 4 uint64s for 256 bits total.
	bitmap [4]uint64
	edges  []*Node[T]
}

// setBit sets the bit for a given label
func setBit(bitmap *[4]uint64, label byte) {
	block := label >> 6
	bitPos := label & 63
	bitmap[block] |= 1 << bitPos
}

// clearBit clears the bit for a given label
func clearBit(bitmap *[4]uint64, label byte) {
	block := label >> 6
	bitPos := label & 63
	mask := uint64(1) << bitPos
	bitmap[block] &^= mask
}

// bitSet checks if bit for label is set
func bitSet(bitmap [4]uint64, label byte) bool {
	block := label >> 6
	bitPos := label & 63
	return (bitmap[block] & (1 << bitPos)) != 0
}

// rankOf computes how many bits are set before foundLabel
func (n *Node[T]) rankOf(foundLabel uint8) int {
	block := foundLabel >> 6
	bitPos := foundLabel & 63
	mask := uint64(1) << bitPos

	rank := 0
	for i := 0; i < int(block); i++ {
		rank += bits.OnesCount64(n.bitmap[i])
	}
	rank += bits.OnesCount64(n.bitmap[block] & (mask - 1))
	return rank
}

// findInsertionIndex finds the index where a label should be inserted.
// Similar to lower bound search in a sorted array, but using a bitmap.
func (n *Node[T]) findInsertionIndex(label byte) int {
	block := label >> 6
	bitPos := label & 63

	// Check current block from bitPos upwards
	curBlock := n.bitmap[block] >> bitPos
	if curBlock != 0 {
		// There is at least one set bit >= bitPos in this block
		offset := bits.TrailingZeros64(curBlock)
		foundLabel := uint8(block*64 + bitPos + uint8(offset))
		if foundLabel >= label {
			return n.rankOf(foundLabel)
		}
	}

	// Check subsequent blocks
	for b := block + 1; b < 4; b++ {
		if n.bitmap[b] != 0 {
			offset := bits.TrailingZeros64(n.bitmap[b])
			foundLabel := uint8(b*64 + uint8(offset))
			// foundLabel > label by definition
			return n.rankOf(foundLabel)
		}
	}

	// No existing child >= label, so insert at end
	return len(n.edges)
}

func (n *Node[T]) addEdge(label byte, child *Node[T]) {
	idx := n.findInsertionIndex(label)
	n.edges = append(n.edges, child)
	if idx != len(n.edges)-1 {
		copy(n.edges[idx+1:], n.edges[idx:len(n.edges)-1])
		n.edges[idx] = child
	}
	setBit(&n.bitmap, label)
}

func (n *Node[T]) replaceEdge(label byte, child *Node[T]) {
	if !bitSet(n.bitmap, label) {
		panic("replacing missing edge")
	}

	// Compute rank
	rank := n.getChildRank(label)
	n.edges[rank] = child
}

func (n *Node[T]) getChildRank(label byte) int {
	block := label >> 6
	bitPos := label & 63
	mask := uint64(1) << bitPos

	rank := 0
	for i := 0; i < int(block); i++ {
		rank += bits.OnesCount64(n.bitmap[i])
	}
	rank += bits.OnesCount64(n.bitmap[block] & (mask - 1))
	return rank
}

func (n *Node[T]) getLowerBoundEdge(label byte) (int, *Node[T]) {
	// Similar logic to find the first child with label >= input
	block := label >> 6
	bitPos := label & 63

	curBlock := n.bitmap[block] >> bitPos
	if curBlock != 0 {
		offset := bits.TrailingZeros64(curBlock)
		foundLabel := block*64 + bitPos + uint8(offset)
		rank := n.rankOf(foundLabel)
		return rank, n.edges[rank]
	}

	for b := block + 1; b < 4; b++ {
		if n.bitmap[b] != 0 {
			offset := bits.TrailingZeros64(n.bitmap[b])
			foundLabel := uint8(b*64 + uint8(offset))
			rank := n.rankOf(foundLabel)
			return rank, n.edges[rank]
		}
	}

	// No child >= label
	return -1, nil
}

func (n *Node[T]) getEdge(label byte) (int, *Node[T]) {
	if !bitSet(n.bitmap, label) {
		return -1, nil
	}
	rank := n.getChildRank(label)
	return rank, n.edges[rank]
}
func (n *Node[T]) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node[T]) delEdge(label byte) {
	if !bitSet(n.bitmap, label) {
		return
	}
	rank := n.getChildRank(label)
	copy(n.edges[rank:], n.edges[rank+1:])
	n.edges[len(n.edges)-1] = nil
	n.edges = n.edges[:len(n.edges)-1]
	clearBit(&n.bitmap, label)
}

func (n *Node[T]) GetWatch(k []byte) (<-chan struct{}, T, bool) {
	search := k
	watch := n.mutateCh
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.mutateCh, n.leaf.val, true
			}
			break
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Update to the finest granularity as the search makes progress
		watch = n.mutateCh

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	var zero T
	return watch, zero, false
}

func (n *Node[T]) Get(k []byte) (T, bool) {
	_, val, ok := n.GetWatch(k)
	return val, ok
}

// LongestPrefix is like Get, but instead of an
// exact match, it will return the longest prefix match.
func (n *Node[T]) LongestPrefix(k []byte) ([]byte, T, bool) {
	var last *leafNode[T]
	search := k
	for {
		// Look for a leaf node
		if n.isLeaf() {
			last = n.leaf
		}

		// Check for key exhaustion
		if len(search) == 0 {
			break
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return last.key, last.val, true
	}
	var zero T
	return nil, zero, false
}

// Minimum is used to return the minimum value in the tree
func (n *Node[T]) Minimum() ([]byte, T, bool) {
	for {
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		}
		if len(n.edges) > 0 {
			n = n.edges[0]
		} else {
			break
		}
	}
	var zero T
	return nil, zero, false
}

// Maximum is used to return the maximum value in the tree
func (n *Node[T]) Maximum() ([]byte, T, bool) {
	for {
		if num := len(n.edges); num > 0 {
			n = n.edges[num-1]
			continue
		}
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		} else {
			break
		}
	}
	var zero T
	return nil, zero, false
}

// Iterator is used to return an iterator at
// the given node to walk the tree
func (n *Node[T]) Iterator() *Iterator[T] {
	return &Iterator[T]{node: n}
}

// ReverseIterator is used to return an iterator at
// the given node to walk the tree backwards
func (n *Node[T]) ReverseIterator() *ReverseIterator[T] {
	return NewReverseIterator(n)
}

// Iterator is used to return an iterator at
// the given node to walk the tree
func (n *Node[T]) PathIterator(path []byte) *PathIterator[T] {
	return &PathIterator[T]{node: n, path: path}
}

// rawIterator is used to return a raw iterator at the given node to walk the
// tree.
func (n *Node[T]) rawIterator() *rawIterator[T] {
	iter := &rawIterator[T]{node: n}
	iter.Next()
	return iter
}

// Walk is used to walk the tree
func (n *Node[T]) Walk(fn WalkFn[T]) {
	recursiveWalk(n, fn)
}

// WalkBackwards is used to walk the tree in reverse order
func (n *Node[T]) WalkBackwards(fn WalkFn[T]) {
	reverseRecursiveWalk(n, fn)
}

// WalkPrefix is used to walk the tree under a prefix
func (n *Node[T]) WalkPrefix(prefix []byte, fn WalkFn[T]) {
	search := prefix
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			recursiveWalk(n, fn)
			return
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			// Child may be under our search prefix
			recursiveWalk(n, fn)
			return
		} else {
			break
		}
	}
}

// WalkPath is used to walk the tree, but only visiting nodes
// from the root down to a given leaf. Where WalkPrefix walks
// all the entries *under* the given prefix, this walks the
// entries *above* the given prefix.
func (n *Node[T]) WalkPath(path []byte, fn WalkFn[T]) {
	i := n.PathIterator(path)

	for path, val, ok := i.Next(); ok; path, val, ok = i.Next() {
		if fn(path, val) {
			return
		}
	}
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalk[T any](n *Node[T], fn WalkFn[T]) bool {
	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	// Iterate over edges
	for _, child := range n.edges {
		if recursiveWalk(child, fn) {
			return true
		}
	}
	return false
}

// reverseRecursiveWalk is used to do a reverse pre-order
// walk of a node recursively. Returns true if the walk
// should be aborted
func reverseRecursiveWalk[T any](n *Node[T], fn WalkFn[T]) bool {
	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	for i := len(n.edges) - 1; i >= 0; i-- {
		if reverseRecursiveWalk(n.edges[i], fn) {
			return true
		}
	}
	return false
}
