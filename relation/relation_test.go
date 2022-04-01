package relation

import (
	"reflect"
	"testing"
)

func TestTupleString(t *testing.T) {
	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20.0"})

	tuple, _ := r.GetRow(0)
	if tuple.String() != "1, John, 20.0" {
		t.Error("Expected 1, John, 20.0, got", tuple.String())
	}
}

func TestTupleEdit(t *testing.T) {
	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20"})

	row, _ := r.GetRow(0)
	*row = *NewTuple([]string{"2", "Jane", "21"})
	newRow, _ := r.GetRow(0)
	if newRow.String() != "2, Jane, 21" {
		t.Error("Expected 2, Jane, 21, got", newRow.String())
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

	tuple, _ := r.GetRow(0)
	variable, _ := (*tuple)[0].Get()
	_, ok := variable.(int64)
	if !ok {
		t.Error("Expected int64, got", reflect.TypeOf(variable))
	}

	variable, _ = (*tuple)[1].Get()
	_, ok = variable.(string)
	if !ok {
		t.Error("Expected string, got", reflect.TypeOf(variable))
	}

	variable, _ = (*tuple)[2].Get()
	_, ok = variable.(float64)
	if !ok {
		t.Error("Expected float64, got", reflect.TypeOf(variable))
	}
}

func TestWrongColumnType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic, got nil")
		}
	}()

	r := NewRelation([]string{"id", "name", "age"})
	r.AddRow([]string{"1", "John", "20.0"})
	r.AddRow([]string{"2", "Jane", "20"})
}
