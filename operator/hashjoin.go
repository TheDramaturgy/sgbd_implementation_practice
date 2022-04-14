package operator

type HashJoin struct {
	child1    Operator
	child2    Operator
	target1   string
	target2   string
	targetID1 int
	targetID2 int
	columns   []string
	opened    bool
}

func NewHashJoin(child1, child2 Operator, target1, target2 string) *HashJoin {
	return &HashJoin{child1: child1, child2: child2, target1: target1,
		target2: target2, opened: false}
}
