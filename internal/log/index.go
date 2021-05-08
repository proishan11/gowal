package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	// Record offset in store
	offWidth uint64 = 4
	// Record position in the store
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// Implements struct abstraction of a log
type index struct {
	// persisted file
	file *os.File
	// memory mapped file for fast lookups
	mmap gommap.MMap
	// size of index / next append location
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Current size of file
	idx.size = uint64(fi.Size())

	// Change the file capacity to MaxIndexBytes
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// Create a memory mapped file from persisted file
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Takes in as offset and return's record position in store
// We store relative offsets, 0 for entry 1, 1 for entry 2
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// last record
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// position of last record in index will be out * entWidth
	pos = uint64(out) * entWidth
	// if position out of bound return EOF
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	// Read offset (decode)
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// Read position (decode)
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// offset of record in store and it's position
func (i *index) Write(off uint32, pos uint64) error {
	// If no space to fill another record
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// Always write encoded entries
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// Sync data with the persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Flush persisted writes
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Truncate to the actual size of the file so that no free space in file
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
