package main

import "fmt"

type Tuple struct {
	data map[string]interface{}
}

type PlanNode interface { /* This is the iterator interface */
	init() error
	next() (Tuple, error)
	close() error
	getInputs() []PlanNode
}

type ScanNode interface {
}

/*** Table ***/

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
	reqHeaders []string
	table      Table
	tableIdx   int
	inputs     []PlanNode
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

	// Confirm if all headers exist
	data := nextTuple.data
	for _, header := range pn.reqHeaders {
		_, exists := data[header]
		if !exists {
			return Tuple{}, fmt.Errorf("header %v doesn't exist in table", header)
		}
	}

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
	tuple := Tuple{}

	if ln.offset >= ln.limit {
		return tuple, nil
	}

	return ln.inputs[0].next()
}

func (ln *LimitNode) close() error {
	return nil
}

func (ln *LimitNode) getInputs() ([]PlanNode, error) {
	return ln.inputs, nil
}
