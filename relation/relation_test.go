package relation

import (
	"reflect"
	"testing"
)

func TestTupleString(t *testing.T) {
	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20.0"})

	if r.GetRow(0).String() != "1, John, 20.0" {
		t.Error("Expected 1, John, 20.0, got", r.GetRow(0).String())
	}
}

func TestTupleEdition(t *testing.T) {
	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20"})

	row := r.GetRow(0)
	*row = NewTuple([]string{"2", "Jane", "21"})
	if r.GetRow(0).String() != "2, Jane, 21" {
		t.Error("Expected 2, Jane, 21, got", r.GetRow(0).String())
	}
}

func TestRowWrongSize(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic, got nil")
		}
	}()

	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20", "3.4"})
}

func TestValueGet(t *testing.T) {
	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20.0"})

	variable, _ := r.GetRow(0).GetValue(0).Get()
	_, ok := variable.(int64)
	if !ok {
		t.Error("Expected int64, got", reflect.TypeOf(variable))
	}

	variable, _ = r.GetRow(0).GetValue(1).Get()
	_, ok = variable.(string)
	if !ok {
		t.Error("Expected string, got", reflect.TypeOf(variable))
	}

	variable, _ = r.GetRow(0).GetValue(2).Get()
	_, ok = variable.(float64)
	if !ok {
		t.Error("Expected float64, got", reflect.TypeOf(variable))
	}
}
