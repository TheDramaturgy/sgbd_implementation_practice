package operator

import (
	rl "sgbd/relation"
	"sgbd/util"
)

type NestedLoopJoin struct {
	child1    Operator
	child2    Operator
	target1   string
	target2   string
	targetID1 int
	targetID2 int
	actual    *rl.Tuple
	columns   []string
	opened    bool
}

// remove removes the element at the given index from the slice of columns.
func remove(columns []string, idx int) []string {
	return append(columns[:idx], columns[idx+1:]...)
}

func NewNestedLoopJoin(child1, child2 Operator, target1, target2 string) *NestedLoopJoin {
	return &NestedLoopJoin{child1: child1, child2: child2, target1: target1,
		target2: target2, opened: false}
}

func (j *NestedLoopJoin) Open() error {
	err := j.child1.Open()
	if err != nil {
		return err
	}

	err = j.child2.Open()
	if err != nil {
		return err
	}

	id1, err := findIndexOfTarget(j.target1, j.child1.(columnGetter).columnGet())
	if err != nil {
		return err
	}

	id2, err := findIndexOfTarget(j.target2, j.child2.(columnGetter).columnGet())
	if err != nil {
		return err
	}

	j.targetID1 = id1
	j.targetID2 = id2
	j.columns = remove(j.child1.(columnGetter).columnGet(), id1)
	j.columns = append(j.columns, j.target1)
	j.columns = append(j.columns, remove(j.child2.(columnGetter).columnGet(), id2)...)
	j.actual, err = j.child1.Next()

	return err
}

func (j *NestedLoopJoin) Next() (*rl.Tuple, error) {
	// if actual tuple is nil, the join already ended
	if j.actual == nil {
		return nil, nil
	}

	for {
		// Get next tuple from second child for merging
		next, err := j.child2.Next()
		util.Check(err)

		// if second child ended, get next tuple from first child
		if next == nil {
			j.actual, err = j.child1.Next()
			util.Check(err)

			if j.actual == nil {
				return nil, nil
			}

			err = j.child2.Close()
			util.Check(err)

			err = j.child2.Open()
			util.Check(err)

			continue
		}

		// if target columns match, then join both tuples
		if (*next)[j.targetID2] == (*j.actual)[j.targetID1] {
			joining := next.Clone()
			joining.Remove(j.targetID2)

			newTuple := j.actual.Clone()
			newTuple.MoveToEnd(j.targetID1)
			newTuple.AppendTuple(joining)

			return newTuple, nil
		}
	}
}

func (j *NestedLoopJoin) Close() error {
	err := j.child1.Close()
	if err != nil {
		return err
	}

	err = j.child2.Close()
	if err != nil {
		return err
	}

	j.targetID1 = -1
	j.targetID2 = -1
	j.actual = nil
	j.opened = false
	return nil
}

func (j *NestedLoopJoin) columnGet() []string {
	return j.columns
}
