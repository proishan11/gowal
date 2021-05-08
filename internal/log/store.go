package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// Encoding used to persist record on disk
	enc = binary.BigEndian
)

const (
	// Size of record in bytes
	lenWidth = 8
)

// Store is where we store the records in
// A segment binds store and an Index
// Each time we run service, we have to create an in memory store from existing
// files (stores on disk)
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// returns size total size written, position where is is written, error
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Current position where the bytes are written
	// Segment uses this in conjuction to the index file to locate records
	pos = s.size

	// Write the record size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	//
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// Size of size written is 8 bytes
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Write any buffered data before reading it
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	// Read size at position
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	// Read data which is located at pos + 8 bytes
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// read p bytes at a given offset
func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
