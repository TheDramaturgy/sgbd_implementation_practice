package operator

type filter struct {
	child   Operator
	target  string
	columns []string
}
