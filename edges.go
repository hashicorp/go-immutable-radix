// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package iradix

type edgeBitMap [4]uint64

// setBit sets the bit for a given label
func (bm *edgeBitMap) setBit(label byte) {
	// Determine which block the label falls into by shifting 6 bits to the right.
	// This is the equivalent of dividing by 64.
	block := label >> 6

	// Since the block captures the two high order bits, we do an AND with the lower 6 bits (0x00111111 == 63)
	// to determine which bit to check within the selected block.
	bitPos := label & 63

	// To set a ith bit at the block, we do a OR(|) operation with 1 << i
	// Left shifting 1 with i, we get a number having 1 at only ith bit
	// all other bits are 0.
	bm[block] |= 1 << bitPos
}

// clearBit clears the bit for a given label
func (bm *edgeBitMap) clearBit(label byte) {
	// Determine which block the label falls into by shifting 6 bits to the right.
	// This is the equivalent of dividing by 64.
	block := label >> 6

	// Since the block captures the two high order bits, we do an AND with the lower 6 bits (0x00111111 == 63)
	// to determine which bit to check within the selected block.
	bitPos := label & 63
	mask := uint64(1) << bitPos
	bm[block] &^= mask
}

// bitSet checks if bit for label is set
func (bm *edgeBitMap) hasBitSet(label byte) bool {
	// Determine which block the label falls into by shifting 6 bits to the right.
	// This is the equivalent of dividing by 64.
	block := label >> 6

	// Since the block captures the two high order bits, we do an AND with the lower 6 bits (0x00111111 == 63)
	// to determine which bit to check within the selected block.
	bitPos := label & 63
	return (bm[block] & (1 << bitPos)) != 0
}
