package operator

import rl "sgbd/relation"

type Operator interface {
	Open() error
	Next() (*rl.Tuple, error)
	Close() error
}

type columnGetter interface {
	columnGet() []string
}
