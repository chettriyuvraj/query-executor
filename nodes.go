package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Tuple struct {
	data map[string]interface{}
}

type PlanNode interface { /* This is the iterator interface, every PlanNode will ideally have an inputs[] array as well, representing sources/children */
	init() error
	next() (Tuple, error)
	close() error
	getInputs() ([]PlanNode, error)
}

type ScanNode interface { // ended up unused, but scan nodes should ideally implement this
}

/*** Table - Represents a mock-up of an actual DB table ***/

type Table struct {
	headers []string
	data    []map[string]interface{} // assume data will always contain keys from headers only
}

func (t *Table) getData(idx int) map[string]interface{} {
	if idx >= len(t.data) {
		return nil
	}
	return t.data[idx]
}

/*** Table Scan Node ***/

type TableScanNode struct {
	table    Table
	tableIdx int
	inputs   []PlanNode
}

func (tn *TableScanNode) init() error {
	return nil
}

func (tn *TableScanNode) next() (Tuple, error) {
	// Get data from table
	data := tn.table.getData(tn.tableIdx)
	tuple := Tuple{data: data}

	// increase table index count
	if data != nil {
		tn.tableIdx++
	}

	return tuple, nil
}

func (tn *TableScanNode) close() error {
	return nil
}

func (tn *TableScanNode) getInputs() ([]PlanNode, error) {
	return tn.inputs, nil
}

/*** File Scan Node ***/

type CSVScanNode struct {
	idx     int
	file    *os.File
	scanner *bufio.Scanner
	path    string
	headers []string
	// delimiter string Assuming newline as the delimiter always for now
	inputs []PlanNode
}

func (fsn *CSVScanNode) init() error {
	file, err := os.Open(fsn.path)
	if err != nil {
		return err
	}

	fsn.scanner = bufio.NewScanner(file)
	fsn.file = file
	dataExists := fsn.scanner.Scan()
	if !dataExists {
		if err := fsn.scanner.Err(); err != nil {
			return err
		}
		return fmt.Errorf("no header row found")
	}
	fsn.headers = strings.Split(fsn.scanner.Text(), ",")
	return nil
}

func (fsn *CSVScanNode) next() (Tuple, error) {
	// Get data from scanner
	dataExists := fsn.scanner.Scan()
	if !dataExists {
		if err := fsn.scanner.Err(); err != nil {
			return Tuple{}, err
		}
		return Tuple{}, nil // EOF
	}

	// Add data to tuple according to headers (assume headers arranged in order of occurrence of field in file)
	tuple := Tuple{}
	textData := strings.Split(fsn.scanner.Text(), ",")
	tuple.data = map[string]interface{}{}
	for i, header := range fsn.headers {
		tuple.data[header] = textData[i]
	}

	fsn.idx++

	return tuple, nil
}

func (fsn *CSVScanNode) close() error {
	return fsn.file.Close()
}

func (fsn *CSVScanNode) getInputs() ([]PlanNode, error) {
	return fsn.inputs, nil
}

/*** Projection Node ***/

type ProjectionNode struct {
	reqHeaders []string
	inputs     []PlanNode
}

func (pn *ProjectionNode) init() error {
	return nil
}

func (pn *ProjectionNode) next() (Tuple, error) {
	// Get next tuple
	nextTuple, err := pn.inputs[0].next()
	if err != nil {
		return Tuple{}, err
	}

	// Confirm if all headers exist and remove remaining headers
	data := nextTuple.data
	newData := map[string]interface{}{}
	if data != nil {
		for _, header := range pn.reqHeaders {
			_, exists := data[header]
			if !exists {
				return Tuple{}, fmt.Errorf("header %v doesn't exist in table", header)
			}
			newData[header] = data[header]
		}
	}
	nextTuple.data = newData

	return nextTuple, nil
}

func (pn *ProjectionNode) close() error {
	return nil
}

func (pn *ProjectionNode) getInputs() ([]PlanNode, error) {
	return pn.inputs, nil
}

/*** Limit Node ***/

type LimitNode struct {
	offset int
	limit  int
	inputs []PlanNode
}

func (ln *LimitNode) init() error {
	return nil
}

func (ln *LimitNode) next() (Tuple, error) {
	if ln.offset >= ln.limit {
		return Tuple{}, nil
	}

	tuple, err := ln.inputs[0].next()
	if err != nil {
		return Tuple{}, err
	}

	ln.offset++
	return tuple, nil
}

func (ln *LimitNode) close() error {
	return nil
}

func (ln *LimitNode) getInputs() ([]PlanNode, error) {
	return ln.inputs, nil
}

/*** Filter Node ***/
type FilterNode struct { // single condition
	header   string // header on which we are checking condition
	operator string
	cmpValue string // assuming all values string for now
	inputs   []PlanNode
}

func (fn *FilterNode) init() error {
	return nil
}

func (fn *FilterNode) next() (Tuple, error) {
	nextTuple, err := fn.inputs[0].next()
	if err != nil {
		return Tuple{}, err
	}

	if nextTuple.data != nil {
		switch op := fn.operator; op {
		case "=":
			value, exists := nextTuple.data[fn.header]
			if !exists {
				return Tuple{}, fmt.Errorf("header %v doesn't exist to filter", fn.header)
			}
			if value != fn.cmpValue {
				return fn.next()
			}
		}
	}

	return nextTuple, nil
}

func (fn *FilterNode) close() error {
	return nil
}

func (fn *FilterNode) getInputs() ([]PlanNode, error) {
	return fn.inputs, nil
}
