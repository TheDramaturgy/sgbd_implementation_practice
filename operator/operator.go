package operator

import (
	"errors"
	rl "sgbd/relation"
)

type Operator interface {
	Open() error
	Next() (*rl.Tuple, error)
	Close() error
}

type columnGetter interface {
	columnGet() []string
}

func findIndexOfTarget(target string, columns []string) (int, error) {
	for i, col := range columns {
		if col == target {
			return i, nil
		}
	}
	return -1, errors.New("target not found: " + target)
}
