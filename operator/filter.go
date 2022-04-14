package operator

import (
	"errors"
	rl "sgbd/relation"
	"strconv"
)

type Filter struct {
	child     Operator
	columns   []string
	condition []string
	opened    bool
}

func (f *Filter) findIndexOfTarget(target string) int {
	for i, col := range f.columns {
		if col == target {
			return i
		}
	}
	return -1
}

func (f *Filter) checkCondition(tuple *rl.Tuple) bool {
	idxColumn := f.findIndexOfTarget(f.condition[0])
	if idxColumn == -1 {
		panic("Column not found. column: " + f.condition[0])
	}

	value, vType := tuple.GetValue(idxColumn).Get()
	switch f.condition[1] {
	case "=":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) == i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) == i
		case rl.TypeOfString():
			return value.(string) == f.condition[2]
		}
	case "!=":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) != i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) != i
		case rl.TypeOfString():
			return value.(string) != f.condition[2]
		}
	case ">":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) > i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) > i
		case rl.TypeOfString():
			return value.(string) > f.condition[2]
		}
	case "<":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) < i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) < i
		case rl.TypeOfString():
			return value.(string) < f.condition[2]
		}
	case ">=":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) >= i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) >= i
		case rl.TypeOfString():
			return value.(string) >= f.condition[2]
		}
	case "<=":
		switch vType {
		case rl.TypeOfInt():
			i, err := strconv.ParseInt(f.condition[2], 10, 64)
			if err != nil {
				return false
			}
			return value.(int64) <= i
		case rl.TypeOfFloat():
			i, err := strconv.ParseFloat(f.condition[2], 64)
			if err != nil {
				return false
			}
			return value.(float64) <= i
		case rl.TypeOfString():
			return value.(string) <= f.condition[2]
		}
	default:
		panic("Invalid operator. operator: " + f.condition[1])
	}
	return false
}

func NewFilter(child Operator, condition []string) *Filter {
	return &Filter{child: child, condition: condition, opened: false}
}

func (f *Filter) Open() error {
	err := f.child.Open()
	if err != nil {
		return err
	} else {
		f.columns = f.child.(columnGetter).columnGet()
	}

	f.opened = true
	return nil
}

func (f *Filter) Next() (*rl.Tuple, error) {
	if !f.opened {
		return nil, errors.New("Filter operator is closed")
	}

	for tuple, err := f.child.Next(); tuple != nil; tuple, err = f.child.Next() {
		if err != nil {
			return nil, err
		}

		if f.checkCondition(tuple) {
			return tuple, nil
		}
	}

	return nil, nil
}

func (f *Filter) Close() error {
	var err error
	if f.child != nil {
		err = f.child.Close()
	}

	f.opened = false
	return err
}

func (f *Filter) columnGet() []string {
	return f.columns
}
