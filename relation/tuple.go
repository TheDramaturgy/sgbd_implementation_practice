package relation

import "reflect"

type Tuple []Value

func NewTuple(values []string) *Tuple {
	t := make(Tuple, 0)
	for _, value := range values {
		t = append(t, NewValue(value))
	}
	return &t
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

func (t *Tuple) Clone() *Tuple {
	newTuple := make(Tuple, 0)
	newTuple = append(newTuple, *t...)
	return &newTuple
}

func (t *Tuple) Remove(idx int) {
	*t = append((*t)[:idx], (*t)[idx+1:]...)
}

func (t *Tuple) AppendValue(value Value) {
	*t = append(*t, value)
}

func (t *Tuple) AppendTuple(tuple *Tuple) {
	*t = append(*t, *tuple...)
}

func (t *Tuple) MoveToEnd(idx int) {
	value := (*t)[idx]
	(*t).Remove(idx)
	(*t).AppendValue(value)
}

func (t *Tuple) GetValue(idx int) Value {
	return (*t)[idx]
}

func (t *Tuple) CheckTypes(columnTypes []reflect.Type) bool {
	for idx, value := range *t {
		if value.type_ != columnTypes[idx] && columnTypes[idx] != reflect.TypeOf(STRING) {
			return false
		}
	}
	return true
}
