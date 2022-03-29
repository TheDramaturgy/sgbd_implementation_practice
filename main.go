package main

import (
	"log"
	op "sgbd/operator"
)

func main() {
	query := op.NewPrint(op.NewScan("data/pessoa.txt", "##"))
	query.Open()

	for tuple, err := query.Next(); tuple != nil; tuple, err = query.Next() {
		if err != nil {
			log.Panic(err)
		}
	}
}
