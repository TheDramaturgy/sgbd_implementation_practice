package operator

import (
	"bufio"
	"errors"
	"os"
	rl "sgbd/relation"
	"strings"
)

type Scan struct {
	path, sep string
	columns   []string
	relation  *rl.Relation
	position  int
	opened    bool
}

// NewScan returns a new Scan operator
func NewScan(path, sep string, columns []string) *Scan {
	return &Scan{path: path, sep: sep, columns: columns, opened: false}
}

// Open starts the Scan operator
func (s *Scan) Open() error {
	file, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer file.Close()

	s.relation = rl.NewRelation(s.columns)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		s.relation.AddRow(strings.Split(line, s.sep))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	s.position = 0
	s.opened = true
	return nil
}

// Next return the next tuple being pointed by the Scan operator
func (s *Scan) Next() (*rl.Tuple, error) {
	if !s.opened {
		return nil, errors.New("Scan operator is closed")
	}

	if s.position >= s.relation.Size() {
		return nil, nil
	}

	tuple, err := s.relation.GetRow(s.position)
	s.position++

	return tuple, err
}

// Close ends the Scan operator
func (s *Scan) Close() error {
	s.relation = nil
	s.opened = false
	s.position = 0
	return nil
}

func (s *Scan) columnGet() []string {
	return s.columns
}
