package main

import (
	"fmt"
	op "sgbd/operator"
)

func main() {
	query := op.NewPrint(op.NewScan("data/pessoa.txt", "##", []string{"pessoaID", "nome"}))
	err := query.Open()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	for tuple, err := query.Next(); tuple != nil; tuple, err = query.Next() {
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}
