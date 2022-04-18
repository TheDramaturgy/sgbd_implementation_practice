package operator

import (
	"fmt"
	rl "sgbd/relation"
)

type SortMergeJoin struct {
	childLeft        *Sort
	childRight       *Sort
	targetLeft       string
	targetRight      string
	targetIDLeft     int
	targetIDRight    int
	currentLeft      *rl.Tuple
	currentRight     *rl.Tuple
	currentBufferIdx int
	buffer           *rl.Relation
	columns          []string
	opened           bool
	debug            bool
	shouldReadBuffer bool
	readFromBuffer   bool
}

// Constructor of SortMergeJoin Operator, it takes two children and the target columns to join.
// The left child must be the minor one, once it will be iterated just once.
func NewSortMergeJoin(childL, childR Operator, targetL, targetR string, debug bool) *SortMergeJoin {
	return &SortMergeJoin{childLeft: NewSort(childL, targetL), childRight: NewSort(childR, targetR),
		targetLeft: targetL, targetRight: targetR, opened: false, debug: debug}
}

func (j *SortMergeJoin) Open() error {
	// Open the children.
	err := j.childLeft.Open()
	if err != nil {
		return err
	}

	err = j.childRight.Open()
	if err != nil {
		return err
	}

	// Find the index for the target columns.
	id1, err := findIndexOfTarget(j.targetLeft, j.childLeft.columnGet())
	if err != nil {
		return err
	}

	id2, err := findIndexOfTarget(j.targetRight, j.childRight.columnGet())
	if err != nil {
		return err
	}

	// get current tuple of each child.
	j.currentLeft, err = j.childLeft.Next()
	if err != nil {
		return err
	}

	j.currentRight, err = j.childRight.Next()
	if err != nil {
		return err
	}

	// initialize the variables needed to the perform the join.
	j.targetIDLeft = id1
	j.targetIDRight = id2
	j.columns = remove(j.childLeft.columnGet(), id1)
	j.columns = append(j.columns, j.targetLeft)
	j.columns = append(j.columns, remove(j.childRight.columnGet(), id2)...)
	j.buffer = rl.NewRelation(j.childRight.columnGet())
	j.currentBufferIdx = 0
	j.shouldReadBuffer = false
	j.readFromBuffer = false
	j.opened = true

	fmt.Println("SortMergeJoin operator is opened")
	return nil
}

func (j *SortMergeJoin) Next() (*rl.Tuple, error) {
	// if the left child is done, then the join has finished.
	if j.currentLeft == nil {
		return nil, nil
	}

	for {
		var err error
		var actual *rl.Tuple

		// ------- Verify if should read buffer or right child --------
		curr1 := j.currentLeft.GetValue(j.targetIDLeft)
		curr2 := rl.NewValue("Empty")
		if j.currentRight != nil {
			curr2 = j.currentRight.GetValue(j.targetIDRight)
		}

		var tupleB *rl.Tuple = nil
		if j.buffer.Size() > 0 && j.currentBufferIdx < j.buffer.Size() {
			if j.debug {
				fmt.Println("  **READING BUFFER: buffer size: ", j.buffer.Size())
				fmt.Println("  **READING BUFFER: buffer Idx: ", j.currentBufferIdx)
			}
			tupleB, err = j.buffer.GetRow(j.currentBufferIdx)
			if err != nil {
				return nil, err
			}
		}

		if j.debug {
			fmt.Println("  **currL:", j.currentLeft)
			fmt.Println("  **currR:", j.currentRight)
			fmt.Println("  **currB:", tupleB)
		}

		if j.shouldReadBuffer {
			if j.debug {
				fmt.Println("SortMergeJoin: reading buffer --------------")
			}
			// if reading from buffer but buffer has ended.
			// then read the next tuple from left child
			if tupleB == nil {
				j.currentLeft, err = j.childLeft.Next()
				if err != nil {
					return nil, err
				}
				if j.currentLeft == nil {
					// then the join has finished.
					return nil, nil
				}

				// if the new tuple from left child has the same target value as the previous one,
				// then continue merging from the start of the buffer;
				// otherwise, clear the buffer and read from the right child.
				t, _ := j.buffer.GetRow(0)
				ab := t.GetValue(j.targetIDRight)
				if curr1 == ab {
					j.currentBufferIdx = 0
					if j.debug {
						fmt.Println("  -> Restart Buffer Reading")
					}
				} else {
					j.shouldReadBuffer = false
					j.buffer.Clear()
					if j.debug {
						fmt.Println("  -> buffer cleared. buffer size: ", j.buffer.Size())
					}
				}
				continue
			}

			// if buffer has not ended, then read the next tuple from buffer.
			ab := tupleB.GetValue(j.targetIDRight)
			if j.debug {
				fmt.Println("  -> comparing: \n     ", j.currentLeft, "\n      -AND-\n     ", tupleB)
			}
			if curr1 != ab {
				if j.debug {
					fmt.Println("  -> Stop Reading Buffer")
				}
				j.shouldReadBuffer = false
				j.buffer.Clear()
				if j.debug {
					fmt.Println("  -> buffer cleared. buffer size: ", j.buffer.Size())
				}
				continue
			}

		} else {
			if j.debug {
				fmt.Println("SortMergeJoin: reading right child --------------")
			}
			// if reading from right child but right child has ended.
			if j.currentRight == nil {
				// if the buffer is not empty, then read the next tuple from buffer.
				if tupleB != nil {
					ab := tupleB.GetValue(j.targetIDRight)
					if curr1 == ab {
						j.shouldReadBuffer = true
						continue
					}
				}

				// otherwise, the join has finished.
				j.currentLeft = nil
				return nil, nil
			}

			if curr1 != curr2 {
				if j.debug {
					fmt.Println("  -> LEFT child != RIGHT child")
				}

				if tupleB != nil {
					if j.debug {
						fmt.Println("  -> Buffer Not Empty")
					}

					j.currentLeft, err = j.childLeft.Next()
					if err != nil {
						return nil, err
					}

					if j.currentLeft == nil {
						return nil, nil
					}

					curr1 = j.currentLeft.GetValue(j.targetIDLeft)
					ab := tupleB.GetValue(j.targetIDRight)
					if curr1 == ab {
						if j.debug {
							fmt.Println("  -> Should read from buffer")
						}

						j.shouldReadBuffer = true

					} else {
						if j.debug {
							fmt.Println("  -> Should not read from buffer")
						}

						j.shouldReadBuffer = false
						j.buffer.Clear()
					}
					continue
				}

				if curr1.LesserThan(curr2) {
					if j.debug {
						fmt.Println("  -> Advancing Left child")
					}

					j.currentLeft, err = j.childLeft.Next()
					if err != nil {
						return nil, err
					}

					if j.currentLeft == nil {
						return nil, nil
					}

					continue
				} else if j.currentRight != nil {
					if j.debug {
						fmt.Println("  -> advancing Right Child")
					}

					j.currentRight, err = j.childRight.Next()
					if err != nil {
						return nil, err
					}

					if j.currentRight == nil {
						j.currentLeft = nil
						return nil, nil
					}

					continue
				}
			}
		}

		// ------- Read Next tuple --------

		if j.shouldReadBuffer {
			actual, err = j.buffer.GetRow(j.currentBufferIdx)
			if err != nil {
				return nil, err
			}

			j.currentBufferIdx++
			j.readFromBuffer = true
			if j.debug {
				fmt.Println("  actual:", actual)
				fmt.Println("  current buffer idx:", j.currentBufferIdx)
			}
		} else {
			actual = j.currentRight

			j.currentRight, err = j.childRight.Next()
			if err != nil {
				return nil, err
			}

			j.readFromBuffer = false
			if j.debug {
				fmt.Println("  ---> actual:", actual)
				fmt.Println("  ---> current right:", j.currentRight)
			}
		}

		// ------- return merged tuple --------

		if (*j.currentLeft)[j.targetIDLeft] == (*actual)[j.targetIDRight] {
			if !j.readFromBuffer {
				j.buffer.AddTuple(actual)
			}

			joining := actual.Clone()
			joining.Remove(j.targetIDRight)

			newTuple := j.currentLeft.Clone()
			newTuple.MoveToEnd(j.targetIDLeft)
			newTuple.AppendTuple(joining)

			return newTuple, nil
		}
	}
}

func (j *SortMergeJoin) Close() error {
	err := j.childLeft.Close()
	if err != nil {
		return err
	}

	err = j.childRight.Close()
	if err != nil {
		return err
	}

	// j.currentLeft = nil
	// j.currentRight = nil
	j.targetIDLeft = -1
	j.targetIDRight = -1
	j.currentBufferIdx = -1
	j.buffer = nil
	j.opened = false
	return nil
}

func (j *SortMergeJoin) columnGet() []string {
	return j.columns
}
