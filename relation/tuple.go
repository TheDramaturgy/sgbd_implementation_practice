package relation

type Tuple []Value

func NewTuple(values []string) Tuple {
	t := make(Tuple, 0)
	for _, value := range values {
		t = append(t, NewValue(value))
	}
	return t
}

func (t *Tuple) String() string {
	result := ""
	for idx, value := range *t {
		result += value.String()
		if idx != len(*t)-1 {
			result += ", "
		}
	}
	return result
}

func (t *Tuple) GetValue(idx int) Value {
	return (*t)[idx]
}
