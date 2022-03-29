package relation

import "strconv"

const STRING VType = "string"
const FLOAT VType = "float"
const INT VType = "int"

type VType string

type Relation struct {
	columns       []string
	columns_types []VType
	rows          [][]interface{}
}

func NewRelation(columns []string) *Relation {
	return &Relation{columns: columns}
}

func (r *Relation) AddRow(row []string) {
	if len(r.rows) == 0 {
		for value := range row {
			_, err := strconv.ParseFloat(row[value], 64)
			if err == nil {
				r.columns_types = append(r.columns_types, FLOAT)
				continue
			}
			_, err = strconv.ParseInt(row[value], 10, 64)
			if err == nil {
				r.columns_types = append(r.columns_types, INT)
				continue
			}
			r.columns_types = append(r.columns_types, STRING)
		}
	}

	new_row := make([]interface{}, len(row))
	for value := range row {
		var err error
		switch r.columns_types[value] {
		case FLOAT:
			new_row[value], err = strconv.ParseFloat(row[value], 64)
			if err != nil {
				panic(err)
			}
		case INT:
			new_row[value], err = strconv.ParseInt(row[value], 10, 64)
			if err != nil {
				panic(err)
			}
		case STRING:
			new_row[value] = row[value]
		}
	}
	r.rows = append(r.rows, new_row)
}

func (r *Relation) GetRow(row int) *[]interface{} {
	return &r.rows[row]
}
