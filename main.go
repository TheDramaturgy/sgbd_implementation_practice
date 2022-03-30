package main

import (
	"log"
	op "sgbd/operator"
)

func main() {
	query := op.NewPrint(op.NewScan("data/pessoa.txt", "##", []string{"pessoaID", "nome"}))
	query.Open()

	for tuple, err := query.Next(); tuple != nil; tuple, err = query.Next() {
		if err != nil {
			log.Panic(err)
		}
	}
}
