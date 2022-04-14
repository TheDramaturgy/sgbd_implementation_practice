package operator

import (
	"errors"
	rl "sgbd/relation"
)

type Projection struct {
	child     Operator
	columns   []string
	target    []string
	idxTarget []int
	opened    bool
}

func (p *Projection) findIndexOfTarget(target string) int {
	for i, col := range p.columns {
		if col == target {
			return i
		}
	}
	return -1
}

func NewProjection(child Operator, target []string) *Projection {
	return &Projection{child: child, target: target, opened: false}
}

func (p *Projection) Open() error {
	err := p.child.Open()
	if err != nil {
		return err
	} else {
		p.columns = p.child.(columnGetter).columnGet()
	}

	p.idxTarget = make([]int, 0)
	for _, t := range p.target {
		i := p.findIndexOfTarget(t)
		if i == -1 {
			return errors.New("Target column not found. target: " + t)
		}
		p.idxTarget = append(p.idxTarget, i)
	}

	p.opened = true
	return nil
}

func (p *Projection) Next() (*rl.Tuple, error) {
	if !p.opened {
		return nil, errors.New("Projection operator is closed")
	}

	tuple, err := p.child.Next()
	if err != nil {
		return nil, err
	}

	if tuple == nil {
		return nil, p.child.Close()
	}

	newTupleValues := make([]string, 0)
	for aux, i := range p.idxTarget {

		if aux != len(p.idxTarget)-1 {
			newTupleValues = append(newTupleValues, (*tuple)[i].String())
		}
	}

	newTuple := rl.NewTuple(newTupleValues)
	return newTuple, nil
}

func (p *Projection) Close() error {
	var err error
	if p.child != nil {
		err = p.child.Close()
	}

	p.idxTarget = nil
	p.opened = false
	return err
}

func (p *Projection) columnGet() []string {
	return p.target
}
