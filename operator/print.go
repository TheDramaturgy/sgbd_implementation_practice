package operator

import (
	"fmt"
	rl "sgbd/relation"
)

type Print struct {
	child   Operator
	columns []string
	opened  bool
}

// return a new Print operator with the given child
func NewPrint(child Operator) *Print {
	return &Print{child: child}
}

// Open starts the operator and its child
func (print *Print) Open() error {
	err := print.child.Open()
	if err != nil {
		print.columns = print.child.(columnGetter).columnGet()
	}
	return err
}

// Next returns the next tuple from the Print operator
func (print *Print) Next() (*rl.Tuple, error) {
	tuple, err := print.child.Next()
	if err != nil {
		return nil, err
	}

	if tuple == nil {
		return nil, print.Close()
	}

	if tuple != nil {
		fmt.Printf("%v \n", tuple)
	}

	return tuple, nil
}

// Close ends the operator and its child
func (print *Print) Close() error {
	var err error
	if print.child != nil {
		err = print.child.Close()
	}

	print = nil
	return err
}
