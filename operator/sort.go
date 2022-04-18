package operator

import (
	"errors"
	"fmt"
	rl "sgbd/relation"
)

var counter int = 0

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
		return err
	}

	s.columns = s.child.(columnGetter).columnGet()
	s.relation = rl.NewRelation(s.columns)

	var t *rl.Tuple
	for t, err = s.child.Next(); t != nil; t, err = s.child.Next() {
		if err != nil {
			return err
		}
		s.relation.AddTuple(t)
	}
	s.child.Close()

	err = s.relation.Sort(s.target)
	if err != nil {
		return err
	}

	s.position = 0
	s.opened = true

	// fileName := "debug/sort_" + fmt.Sprintf("%d", counter) + ".txt"
	// f, err := os.Create(fileName)
	// counter += 1
	// defer f.Close()
	// if err != nil {
	// 	panic(err)
	// }
	// w := bufio.NewWriter(f)
	// for idx, t := range s.relation.Rows() {
	// 	str := fmt.Sprintf("%d: %v\n", idx, t)
	// 	w.WriteString(str)
	// }
	// w.Flush()

	return nil
}

func (s *Sort) Next() (*rl.Tuple, error) {
	if !s.opened {
		return nil, errors.New("Sort operator is closed")
	}

	if s.position >= len(s.relation.Rows()) {
		return nil, nil
	}

	tuple, err := s.relation.GetRow(s.position)
	s.position++

	return tuple, err
}

func (s *Sort) Close() error {
	var err error
	if s.child != nil {
		err = s.child.Close()
	}

	s.relation = nil
	s.opened = false
	s.position = 0
	return err
}

func (s *Sort) columnGet() []string {
	return s.columns
}
