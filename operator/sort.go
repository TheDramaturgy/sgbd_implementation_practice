package operator

import (
	"errors"
	rl "sgbd/relation"
)

type Sort struct {
	child    Operator
	target   string
	columns  []string
	relation *rl.Relation
	position int
	opened   bool
}

func NewSort(child Operator, target string) *Sort {
	return &Sort{child: child, target: target, opened: false}
}

func (s *Sort) Open() error {
	err := s.child.Open()
	if err != nil {
		s.columns = s.child.(columnGetter).columnGet()
		s.relation = rl.NewRelation(s.columns)
	} else {
		return err
	}

	var t *rl.Tuple
	for t, err = s.child.Next(); t != nil; t, err = s.child.Next() {
		s.relation.AddTuple(t)
	}

	s.relation.Sort(s.target)
	return err
}

func (s *Sort) Next() (*rl.Tuple, error) {
	if !s.opened {
		return nil, errors.New("Sort operator is closed")
	}

	if s.position >= len(s.relation.Rows()) {
		s.Close()
		return nil, nil
	}

	tuple := s.relation.GetRow(s.position)
	s.position++

	return tuple, nil
}

func (s *Sort) Close() error {
	var err error
	if s.child != nil {
		err = s.child.Close()
	}

	s = nil
	return err
}
