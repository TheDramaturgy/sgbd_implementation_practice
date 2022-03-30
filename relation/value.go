package relation

import (
	"reflect"
	"strconv"
)

const STRING string = ""
const FLOAT float64 = 0.0
const INT int64 = 0

type Value struct {
	value_ string
	type_  reflect.Type
}

func findValueType(value string) reflect.Type {
	_, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return reflect.TypeOf(INT)
	}

	_, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return reflect.TypeOf(FLOAT)
	}

	return reflect.TypeOf(STRING)
}

func NewValue(value string) Value {
	return Value{value_: value, type_: findValueType(value)}
}

func (v Value) String() string {
	return v.value_
}

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

func TypeOfInt() reflect.Type {
	return reflect.TypeOf(INT)
}

func TypeOfFloat() reflect.Type {
	return reflect.TypeOf(FLOAT)
}

func TypeOfString() reflect.Type {
	return reflect.TypeOf(STRING)
}
