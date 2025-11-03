// Copyright IBM Corp. 2015, 2025
// SPDX-License-Identifier: MPL-2.0

package iradix

import (
	"testing"
)

func TestNodeWalk(t *testing.T) {
	r := New[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	i := 0

	r.Root().Walk(func(k []byte, _ any) bool {
		got := string(k)
		want := keys[i]
		if got != want {
			t.Errorf("got %s, want: %s", got, want)
		}

		i++
		return i >= len(keys)
	})
}

func TestNodeWalkBackwards(t *testing.T) {
	r := New[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	i := len(keys) - 1

	r.Root().WalkBackwards(func(k []byte, _ any) bool {
		got := string(k)
		want := keys[i]
		if got != want {
			t.Errorf("got %s, want: %s", got, want)
		}

		i--
		return i < 0
	})
}
