package operator

import "fmt"

type Print struct {
	child Operator
}

func NewPrint(child Operator) *Print {
	return &Print{child: child}
}

func (print *Print) Open() error {
	err := print.child.Open()
	return err
}

func (print *Print) Next() (*[]string, error) {
	tuple, err := print.child.Next()
	if err != nil {
		return nil, err
	}

	if tuple == nil {
		return nil, print.Close()
	}

	if tuple != nil {
		fmt.Printf("%v ", tuple)
	}
	fmt.Println()

	return tuple, nil
}

func (print *Print) Close() error {
	print.child.Close()
	return nil
}
