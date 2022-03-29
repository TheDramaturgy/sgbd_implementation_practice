package relation

import (
	"reflect"
	"strconv"
	"strings"
)

type Relation struct {
	columns      []string
	columnsTypes []reflect.Type
	rows         []*Tuple
}

// constructor for relation
func NewRelation(columns []string) *Relation {
	return &Relation{columns: columns}
}

// findColumnTypes finds the type of each column in the relation
// usually called by AddRow when the first row is added to the relation
func (r *Relation) findColumnTypes(row []string) {
	for value := range row {
		// Try parsing as int, if no error occurs then it is an int
		_, err := strconv.ParseInt(row[value], 10, 64)
		if err == nil {
			r.columnsTypes = append(r.columnsTypes, reflect.TypeOf(INT))
			continue
		}

		// Try parsing as float, if no error occurs then it is a float
		_, err = strconv.ParseFloat(row[value], 64)
		if err == nil {
			r.columnsTypes = append(r.columnsTypes, reflect.TypeOf(FLOAT))
			continue
		}

		// If it is neither a float nor an int then it is a string
		r.columnsTypes = append(r.columnsTypes, reflect.TypeOf(STRING))
	}
}

// AddRow adds a row to the relation
func (r *Relation) AddRow(row []string) {
	// if it is the first row added to the relation,
	// then find the types of each column
	if len(r.rows) == 0 {
		r.findColumnTypes(row)
	}

	// if the row size does not match the relation columns size
	// then the row cannot be added to the relation
	if len(row) != len(r.columns) {
		panic("Row size does not match relation size. Row: " + strings.Join(row, ", "))
	}

	t := NewTuple(row)
	if t.CheckTypes(r.columnsTypes) {
		r.rows = append(r.rows, t)
	} else {
		strCT := ""
		for _, v := range r.columnsTypes {
			strCT += v.String() + ", "
		}
		strRow := strings.Join(row, ", ")
		panic("Row types do not match relation types.\n  Row: " + strRow + "\n  ColumnTypes: " + strCT)
	}
}

func (r *Relation) AddTuple(t *Tuple) {
	r.rows = append(r.rows, t)
}

// GetColumns returns the columns of the relation with the informed index
func (r *Relation) GetRow(idx int) *Tuple {
	return r.rows[idx]
}

func (r *Relation) Rows() []*Tuple {
	return r.rows
}
