package operator

type Operator interface {
	Open() error
	Next() (*[]string, error)
	Close() error
}
