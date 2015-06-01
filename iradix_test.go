package iradix

import (
	crand "crypto/rand"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func NewFromMap(m map[string]interface{}) *Tree {
	t := New()
	txn := t.Txn()
	for k, v := range m {
		txn.Insert([]byte(k), v)
	}
	return txn.Commit()
}

func TestRadix(t *testing.T) {
	var min, max string
	inp := make(map[string]interface{})
	for i := 0; i < 1000; i++ {
		gen := generateUUID()
		inp[gen] = i
		if gen < min || i == 0 {
			min = gen
		}
		if gen > max || i == 0 {
			max = gen
		}
	}

	r := NewFromMap(inp)
	if r.Len() != len(inp) {
		t.Fatalf("bad length: %v %v", r.Len(), len(inp))
	}

	for k, v := range inp {
		out, ok := r.Get([]byte(k))
		if !ok {
			t.Fatalf("missing key: %v", k)
		}
		if out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}

	// Check min and max
	outMin, _, _ := r.Minimum()
	if string(outMin) != min {
		t.Fatalf("bad minimum: %v %v", outMin, min)
	}
	outMax, _, _ := r.Maximum()
	if string(outMax) != max {
		t.Fatalf("bad maximum: %v %v", outMax, max)
	}

	for k, v := range inp {
		tree, out, ok := r.Delete([]byte(k))
		r = tree
		if !ok {
			t.Fatalf("missing key: %v", k)
		}
		if out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("bad length: %v", r.Len())
	}
}

func TestRoot(t *testing.T) {
	r := New()
	_, _, ok := r.Delete(nil)
	if ok {
		t.Fatalf("bad")
	}
	_, _, ok = r.Insert(nil, true)
	if ok {
		t.Fatalf("bad")
	}
	val, ok := r.Get(nil)
	if !ok || val != true {
		t.Fatalf("bad: %v", val)
	}
	_, val, ok = r.Delete(nil)
	if !ok || val != true {
		t.Fatalf("bad: %v", val)
	}
}

func TestDelete(t *testing.T) {

	r := New()

	s := []string{"", "A", "AB"}

	for _, ss := range s {
		r.Insert([]byte(ss), true)
	}

	for _, ss := range s {
		_, _, ok := r.Delete([]byte(ss))
		if !ok {
			t.Fatalf("bad %q", ss)
		}
	}
}

func TestLongestPrefix(t *testing.T) {
	r := New()

	keys := []string{
		"",
		"foo",
		"foobar",
		"foobarbaz",
		"foobarbazzip",
		"foozip",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if r.Len() != len(keys) {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	type exp struct {
		inp string
		out string
	}
	cases := []exp{
		{"a", ""},
		{"abc", ""},
		{"fo", ""},
		{"foo", "foo"},
		{"foob", "foo"},
		{"foobar", "foobar"},
		{"foobarba", "foobar"},
		{"foobarbaz", "foobarbaz"},
		{"foobarbazzi", "foobarbaz"},
		{"foobarbazzip", "foobarbazzip"},
		{"foozi", "foo"},
		{"foozip", "foozip"},
		{"foozipzap", "foozip"},
	}
	for _, test := range cases {
		m, _, ok := r.LongestPrefix([]byte(test.inp))
		if !ok {
			t.Fatalf("no match: %v", test)
		}
		if string(m) != test.out {
			t.Fatalf("mis-match: %v %v", m, test)
		}
	}
}

func TestWalkPrefix(t *testing.T) {
	r := New()

	keys := []string{
		"foobar",
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"zipzap",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if r.Len() != len(keys) {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	type exp struct {
		inp string
		out []string
	}
	cases := []exp{
		exp{
			"f",
			[]string{"foobar", "foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		exp{
			"foo",
			[]string{"foobar", "foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		exp{
			"foob",
			[]string{"foobar"},
		},
		exp{
			"foo/",
			[]string{"foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		exp{
			"foo/b",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		exp{
			"foo/ba",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		exp{
			"foo/bar",
			[]string{"foo/bar/baz"},
		},
		exp{
			"foo/bar/baz",
			[]string{"foo/bar/baz"},
		},
		exp{
			"foo/bar/bazoo",
			[]string{},
		},
		exp{
			"z",
			[]string{"zipzap"},
		},
	}

	for _, test := range cases {
		out := []string{}
		fn := func(k []byte, v interface{}) bool {
			out = append(out, string(k))
			return false
		}
		r.WalkPrefix([]byte(test.inp), fn)
		sort.Strings(out)
		sort.Strings(test.out)
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %v %v", out, test.out)
		}
	}
}

func TestWalkPath(t *testing.T) {
	r := New()

	keys := []string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"zipzap",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if r.Len() != len(keys) {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	type exp struct {
		inp string
		out []string
	}
	cases := []exp{
		exp{
			"f",
			[]string{},
		},
		exp{
			"foo",
			[]string{"foo"},
		},
		exp{
			"foo/",
			[]string{"foo"},
		},
		exp{
			"foo/ba",
			[]string{"foo"},
		},
		exp{
			"foo/bar",
			[]string{"foo", "foo/bar"},
		},
		exp{
			"foo/bar/baz",
			[]string{"foo", "foo/bar", "foo/bar/baz"},
		},
		exp{
			"foo/bar/bazoo",
			[]string{"foo", "foo/bar", "foo/bar/baz"},
		},
		exp{
			"z",
			[]string{},
		},
	}

	for _, test := range cases {
		out := []string{}
		fn := func(k []byte, v interface{}) bool {
			out = append(out, string(k))
			return false
		}
		r.WalkPath([]byte(test.inp), fn)
		sort.Strings(out)
		sort.Strings(test.out)
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %v %v", out, test.out)
		}
	}
}

// generateUUID is used to generate a random UUID
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}
