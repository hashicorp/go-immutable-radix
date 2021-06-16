go-immutable-radix [![CircleCI](https://circleci.com/gh/hashicorp/go-immutable-radix/tree/master.svg?style=svg)](https://circleci.com/gh/hashicorp/go-immutable-radix/tree/master)
=========

Provides the `iradix` package that implements an immutable [radix tree](http://en.wikipedia.org/wiki/Radix_tree).
The package only provides a single `Tree` implementation, optimized for sparse nodes.

As a radix tree, it provides the following:
 * O(k) operations. In many cases, this can be faster than a hash table since
   the hash function is an O(k) operation, and hash tables have very poor cache locality.
 * Minimum / Maximum value lookups
 * Ordered iteration

A tree supports using a transaction to batch multiple updates (insert, delete)
in a more efficient manner than performing each operation one at a time.

For a mutable variant, see [go-radix](https://github.com/armon/go-radix).

## :warning: This tree implementation is low-level and can be easily used incorrectly
   - **No key may be a prefix of any other.**
     - This is _not_ enforced but _will cause undefined behavior_ if violated. 
     - The typical way to work around this is to append a null byte (`0x0`) to each key unless you know in advance your keys will never be prefixes. We don't always do this in tests if we arranged them to not be prefixes anyway!
     - We could avoid this gotcha by automatically adding null bytes to all keys but that has high cost of causing re-allocation of keys for every operation that the caller might not want if they already.

Documentation
=============

The full documentation is available on [Godoc](http://godoc.org/github.com/hashicorp/go-immutable-radix).

Example
=======

Below is a simple example of usage

```go
// Create a tree
r := iradix.New()
r, _, _ = r.Insert([]byte("foo\x00"), 1)
r, _, _ = r.Insert([]byte("bar\x00"), 2)
r, _, _ = r.Insert([]byte("foobar\x00"), 2)

// Find the longest prefix match
m, _, _ := r.Root().LongestPrefix([]byte("foozip"))
if string(m) != "foo" {
    panic("should be foo")
}
```

Here is an example of performing a range scan of the keys.

```go
// Create a tree
r := iradix.New()
r, _, _ = r.Insert([]byte("001\x00"), 1)
r, _, _ = r.Insert([]byte("002\x00"), 2)
r, _, _ = r.Insert([]byte("005\x00"), 5)
r, _, _ = r.Insert([]byte("010\x00"), 10)
r, _, _ = r.Insert([]byte("100\x00"), 10)

// Range scan over the keys that sort lexicographically between [003, 050)
it := r.Root().Iterator()
it.SeekLowerBound([]byte("003"))
for key, _, ok := it.Next(); ok; key, _, ok = it.Next() {
  if key >= "005" {
      break
  }
  fmt.Println(key)
}
// Output:
//  005
//  010
```

