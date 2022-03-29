package operator

import (
	"bufio"
	"os"
	"strings"
)

type Scan struct {
	path, sep string
	data      [][]string
	position  int
}

func NewScan(path, sep string) *Scan {
	return &Scan{path: path, sep: sep}
}

func (scan *Scan) Open() error {
	file, err := os.Open(scan.path)
	if err != nil {
		return err
	}
	defer file.Close()

	scan.position = 0
	scan.data = make([][]string, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		scan.data = append(scan.data, strings.Split(line, scan.sep))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (scan *Scan) Next() (*[]string, error) {
	if scan.position >= len(scan.data) {
		return nil, nil
	}

	tuple := scan.data[scan.position]
	scan.position++
	return &tuple, nil
}

func (scan *Scan) Close() error {
	scan = nil
	return nil
}
