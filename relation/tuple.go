package relation

import "reflect"

// Tuple is a slice of Value.
type Tuple []Value

// Constructor of Tuple type.
func NewTuple(values []string) *Tuple {
	// Dynamically allocate a new Tuple variable.
	t := make(Tuple, 0)

	// For each value in values param, we create a new dynamic Value and append it to the Tuple.
	for _, value := range values {
		t = append(t, NewValue(value))
	}

	return &t
}

// String returns the string representation of a Tuple.
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

// Clone returns a copy of the Tuple.
func (t *Tuple) Clone() *Tuple {
	newTuple := make(Tuple, 0)
	newTuple = append(newTuple, *t...)
	return &newTuple
}

// Remove removes the value at the given index.
func (t *Tuple) Remove(idx int) {
	*t = append((*t)[:idx], (*t)[idx+1:]...)
}

// AppendValue appends a given value to the end of the Tuple.
func (t *Tuple) AppendValue(value Value) {
	*t = append(*t, value)
}

// AppendTuple appends the values of a given Tuple to the end of the Tuple.
func (t *Tuple) AppendTuple(tuple *Tuple) {
	*t = append(*t, *tuple...)
}

// MoveToEnd moves the value at the given index to the end of the Tuple.
func (t *Tuple) MoveToEnd(idx int) {
	// Checks if given index is valid.
	if idx >= len(*t) {
		panic("index out of range")
	}

	value := (*t)[idx]
	(*t).Remove(idx)
	(*t).AppendValue(value)
}

// GetValue returns the value at the given index.
func (t *Tuple) GetValue(idx int) Value {
	return (*t)[idx]
}

// SetValue sets the value at the given index.
func (t *Tuple) SetValue(idx int, value string) {
	(*t)[idx] = NewValue(value)
}

// CheckTypes verifies if all the values in a tuple corresponds to the given slice.
func (t *Tuple) CheckTypes(columnTypes []reflect.Type) bool {
	if len(*t) != len(columnTypes) {
		return false
	}

	for idx, value := range *t {
		// As string is a default type, we don't need to check it.
		if columnTypes[idx] == reflect.TypeOf(STRING) {
			continue
		}

		if value.type_ != columnTypes[idx] {
			return false
		}
	}

	return true
}
