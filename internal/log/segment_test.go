package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/proishan11/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	// 3 records max can be inserted
	c.Segment.MaxIndexBytes = entWidth * 3
	c.Segment.MaxStoreBytes = 1024

	// baseOffset = 16
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
	// After appending 3 records, segment should maxed out
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())

	// 3 records max in store
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// Should return true as store is nmaxed out
	require.True(t, s.IsMaxed())
	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
