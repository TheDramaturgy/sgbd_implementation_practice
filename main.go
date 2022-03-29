package main

import (
	rl "sgbd/relation"
)

func main() {
	// query := op.NewPrint(op.NewScan("data/pessoa.txt", "##"))
	// query.Open()

	// for tuple, err := query.Next(); tuple != nil; tuple, err = query.Next() {
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// }

	relation := rl.NewRelation([]string{"id", "nome", "idade"})
	relation.AddRow([]string{"1", "João", "20"})
	relation.AddRow([]string{"2", "Maria", "25"})

	row := relation.GetRow(0)
	print(row)
	row2 := relation.GetRow(0)
	print(row2)
}
