package relation

import (
	"reflect"
	"strconv"
)

const STRING string = ""
const FLOAT float64 = 0.0
const INT int64 = 0

// Value is a dynamic type, it can store values of type int, float64 and string.
type Value struct {
	value_ string
	type_  reflect.Type
}

// Tries to find and return the type of a given value in a string.
// If the value is not a number, it returns the type of a string.
func findValueType(value string) reflect.Type {
	// Once a float cannot be converted into integer, it is the first type we try.
	_, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return reflect.TypeOf(INT)
	}

	// Then we try to convert into float.
	_, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return reflect.TypeOf(FLOAT)
	}

	// If the values is not a number, we use it as a string.
	return reflect.TypeOf(STRING)
}

// Constructor for the Value type.
func NewValue(value string) Value {
	return Value{value_: value, type_: findValueType(value)}
}

// Returns the content of a Value in string format.
func (v Value) String() string {
	return v.value_
}

// Returns the content of Value and its type.
func (v Value) Get() (any, reflect.Type) {
	switch v.type_ {
	case reflect.TypeOf(FLOAT):
		cValue, _ := strconv.ParseFloat(v.value_, 64)
		return cValue, TypeOfFloat()
	case reflect.TypeOf(INT):
		cValue, _ := strconv.ParseInt(v.value_, 10, 64)
		return cValue, TypeOfInt()
	default:
		return v.value_, TypeOfString()
	}
}

// LesserThan returns true if a value is lesser than the other, and false otherwise.
func (v Value) LesserThan(other Value) bool {
	// If the types are different, they are different.
	if v.type_ != other.type_ {
		return false
	}

	switch v.type_ {
	case reflect.TypeOf(FLOAT):
		lValue, _ := strconv.ParseFloat(v.value_, 64)
		rValue, _ := strconv.ParseFloat(other.value_, 64)
		return lValue < rValue
	case reflect.TypeOf(INT):
		lValue, _ := strconv.ParseInt(v.value_, 10, 64)
		rValue, _ := strconv.ParseInt(other.value_, 10, 64)
		return lValue < rValue
	default:
		return v.value_ < other.value_
	}
}

// GreaterThan returns true if a value is greater than the other, and false otherwise.
func (v Value) GreaterThan(other Value) bool {
	// If the types are different, they are different.
	if v.type_ != other.type_ {
		return false
	}

	switch v.type_ {
	case reflect.TypeOf(FLOAT):
		lValue, _ := strconv.ParseFloat(v.value_, 64)
		rValue, _ := strconv.ParseFloat(other.value_, 64)
		return lValue > rValue
	case reflect.TypeOf(INT):
		lValue, _ := strconv.ParseInt(v.value_, 10, 64)
		rValue, _ := strconv.ParseInt(other.value_, 10, 64)
		return lValue > rValue
	default:
		return v.value_ > other.value_
	}
}

// TypeOfInt returns the reflection type of an int.
func TypeOfInt() reflect.Type {
	return reflect.TypeOf(INT)
}

// TypeOfFloat returns the reflection type of a float.
func TypeOfFloat() reflect.Type {
	return reflect.TypeOf(FLOAT)
}

// TypeOfString returns the reflection type of a string.
func TypeOfString() reflect.Type {
	return reflect.TypeOf(STRING)
}
