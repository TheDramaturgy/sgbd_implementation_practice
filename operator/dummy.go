package operator

import (
	"errors"
	rl "sgbd/relation"
)

type Dummy struct {
	columns  []string
	relation *rl.Relation
	position int
	opened   bool
}

func NewDummy(columns []string, relation *rl.Relation) *Dummy {
	return &Dummy{columns: columns, relation: relation, opened: false}
}

func (d *Dummy) Open() error {
	d.opened = true
	d.position = 0
	return nil
}

func (d *Dummy) Next() (*rl.Tuple, error) {
	if !d.opened {
		return nil, errors.New("Dummy operator is closed")
	}

	if d.position >= len(d.relation.Rows()) {
		return nil, nil
	}

	tuple, err := d.relation.GetRow(d.position)
	d.position++

	return tuple, err
}

func (d *Dummy) Close() error {
	d.opened = false
	return nil
}

func (d *Dummy) columnGet() []string {
	return d.columns
}
