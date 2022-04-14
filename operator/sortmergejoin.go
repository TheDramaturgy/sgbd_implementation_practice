package operator

import (
	rl "sgbd/relation"
)

type SortMergeJoin struct {
	child1           *Sort
	child2           *Sort
	target1          string
	target2          string
	targetID1        int
	targetID2        int
	actual1          *rl.Tuple
	actual2          *rl.Tuple
	actualB          int
	buffer           *rl.Relation
	columns          []string
	opened           bool
	shouldReadBuffer bool
	hasBufferEnded   bool
}

func NewSortMergeJoin(child1, child2 Operator, target1, target2 string) *SortMergeJoin {
	return &SortMergeJoin{child1: NewSort(child1, target1), child2: NewSort(child2, target2),
		target1: target1, target2: target2, opened: false}
}

func (j *SortMergeJoin) Open() error {
	err := j.child1.Open()
	if err != nil {
		return err
	}

	err = j.child2.Open()
	if err != nil {
		return err
	}

	id1, err := findIndexOfTarget(j.target1, j.child1.columnGet())
	if err != nil {
		return err
	}

	id2, err := findIndexOfTarget(j.target2, j.child2.columnGet())
	if err != nil {
		return err
	}

	j.actual1, err = j.child1.Next()
	if err != nil {
		return err
	}

	j.actual2, err = j.child2.Next()
	if err != nil {
		return err
	}

	j.targetID1 = id1
	j.targetID2 = id2
	j.columns = remove(j.child1.columnGet(), id1)
	j.columns = append(j.columns, j.target1)
	j.columns = append(j.columns, remove(j.child2.columnGet(), id2)...)
	j.buffer = rl.NewRelation(j.child2.columnGet())
	j.actualB = 0
	j.shouldReadBuffer = false
	j.hasBufferEnded = false

	return nil
}

func (j *SortMergeJoin) Next() (*rl.Tuple, error) {
	if j.actual1 == nil {
		return nil, nil
	}

	for {
		var err error
		var actual *rl.Tuple

		// ------- Verify if should read buffer or child2 --------
		a1 := j.actual1.GetValue(j.targetID1)
		a2 := j.actual2.GetValue(j.targetID2)
		tupleB, err := j.buffer.GetRow(j.actualB)
		if err != nil {
			return nil, err
		}

		if j.shouldReadBuffer {

		} else {
			if a1 != a2 {
				if tupleB != nil {
					ab := tupleB.GetValue(j.targetID2)
					if a1 == ab {
						j.shouldReadBuffer = true
						continue
					}
				}

				if a1.LesserThan(a2) {
					j.actual1, err = j.child1.Next()
					if err != nil {
						return nil, err
					}
					if j.actual1 == nil {
						return nil, nil
					}
					continue
				} else if j.actual2 != nil {
					j.actual2, err = j.child2.Next()
					if err != nil {
						return nil, err
					}
				}
			}
		}

		// ------- Read Next tuple --------

		if j.shouldReadBuffer {
			actual, err = j.buffer.GetRow(j.actualB)
			if err != nil {
				return nil, err
			}

			j.actualB++
			if j.actualB >= len(j.buffer.Rows()) {
				j.hasBufferEnded = true
			}
		} else {
			actual = j.actual2

			j.actual2, err = j.child2.Next()
			if err != nil {
				return nil, err
			}
		}

		// ------- return new tuple --------
		if (*j.actual1)[j.targetID1] == (*actual)[j.targetID2] {
			j.buffer.AddTuple(actual)

			joining := actual.Clone()
			joining.Remove(j.targetID2)

			newTuple := j.actual1.Clone()
			newTuple.MoveToEnd(j.targetID1)
			newTuple.AppendTuple(joining)

			return newTuple, nil
		}
	}
}

func (j *SortMergeJoin) Close() error {
	err := j.child1.Close()
	if err != nil {
		return err
	}

	err = j.child2.Close()
	if err != nil {
		return err
	}

	j.actual1 = nil
	j.actual2 = nil
	j.targetID1 = -1
	j.targetID2 = -1
	j.actualB = -1
	j.buffer = nil
	j.opened = false
	return nil
}

func (j *SortMergeJoin) columnGet() []string {
	return j.columns
}
