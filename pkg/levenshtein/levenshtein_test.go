package levenshtein

import "testing"

func TestHasPrefix(t *testing.T) {
	type args struct {
		s         []byte
		prefix    []byte
		threshold int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "exact",
			args: args{
				s:         []byte("exact"),
				prefix:    []byte("exact"),
				threshold: 1,
			},
			want: true,
		},
		{
			name: "depth 1, same len",
			args: args{
				s:         []byte("exect"),
				prefix:    []byte("exact"),
				threshold: 1,
			},
			want: true,
		},
		{
			name: "depth 2, same len, threshold 1",
			args: args{
				s:         []byte("exeet"),
				prefix:    []byte("exact"),
				threshold: 1,
			},
			want: false,
		},
		{
			name: "depth 3, same len, threshold 3",
			args: args{
				s:         []byte("eeeet"),
				prefix:    []byte("exact"),
				threshold: 3,
			},
			want: true,
		},
		{
			name: "short string, depth 1, same len, threshold 1",
			args: args{
				s:         []byte("eea"),
				prefix:    []byte("exa"),
				threshold: 1,
			},
			want: true,
		},
		{
			name: "depth 1, same len, threshold 1",
			args: args{
				s:         []byte("greee"),
				prefix:    []byte("greek"),
				threshold: 1,
			},
			want: true,
		},
		{
			name: "equal strings, same len, threshold 0",
			args: args{
				s:         []byte("equal"),
				prefix:    []byte("equal"),
				threshold: 0,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, got := HasPrefix(tt.args.s, tt.args.prefix, tt.args.threshold); got != tt.want {
				t.Errorf("HasPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}
