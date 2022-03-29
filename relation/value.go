package relation

import "strconv"

const STRING vType = "string"
const FLOAT vType = "float64"
const INT vType = "int64"

type vType string

func (v vType) String() string {
	return string(v)
}

type Value struct {
	value_ string
	type_  vType
}

func findValueType(value string) vType {
	_, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return INT
	}

	_, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return FLOAT
	}

	return STRING
}

func NewValue(value string) Value {
	return Value{value_: value, type_: findValueType(value)}
}

func (v Value) String() string {
	return v.value_
}

func (v Value) Get() (any, string) {
	switch v.type_ {
	case FLOAT:
		cValue, _ := strconv.ParseFloat(v.value_, 64)
		return cValue, FLOAT.String()
	case INT:
		cValue, _ := strconv.ParseInt(v.value_, 10, 64)
		return cValue, INT.String()
	default:
		return v.value_, STRING.String()
	}
}
