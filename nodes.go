package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/chettriyuvraj/query-executor/ycfile"
)

const PAGESIZE = 8196

type Tuple struct {
	data map[string]interface{}
}

type PlanNode interface { /* This is the iterator interface, every PlanNode will ideally have an inputs[] array as well, representing sources/children */
	init() error
	next() (Tuple, error)
	close() error
	reset() error
	getInputs() ([]PlanNode, error)
	setInputs(inps []PlanNode)
}

func resetPlanNode(pn PlanNode) error {
	inps, err := pn.getInputs()
	if err != nil {
		return err
	}

	for _, inp := range inps {
		err := inp.reset()
		if err != nil {
			return err
		}
	}

	return err
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

func (tn *TableScanNode) reset() error {
	return resetPlanNode(tn)
}

func (tn *TableScanNode) setInputs(inps []PlanNode) {
	tn.inputs = inps
}

/*** CSV Scan Node ***/

type CSVScanNode struct {
	idx     int
	file    *os.File
	scanner *bufio.Scanner
	path    string
	headers []string
	// delimiter string Assuming newline as the delimiter always for now
	inputs []PlanNode
}

func (csvn *CSVScanNode) init() error {
	file, err := os.Open(csvn.path)
	if err != nil {
		return err
	}

	csvn.scanner = bufio.NewScanner(file)
	csvn.file = file
	dataExists := csvn.scanner.Scan()
	if !dataExists {
		if err := csvn.scanner.Err(); err != nil {
			return err
		}
		return fmt.Errorf("no header row found")
	}
	csvn.headers = strings.Split(csvn.scanner.Text(), ",")
	return nil
}

func (csvn *CSVScanNode) next() (Tuple, error) {
	// Get data from scanner
	dataExists := csvn.scanner.Scan()
	if !dataExists {
		if err := csvn.scanner.Err(); err != nil {
			return Tuple{}, err
		}
		return Tuple{}, nil // EOF
	}

	// Add data to tuple according to headers (assume headers arranged in order of occurrence of field in file)
	tuple := Tuple{}
	textData := strings.Split(csvn.scanner.Text(), ",")
	tuple.data = map[string]interface{}{}
	for i, header := range csvn.headers {
		tuple.data[header] = textData[i]
	}

	csvn.idx++

	return tuple, nil
}

func (csvn *CSVScanNode) close() error {
	return csvn.file.Close()
}

func (csvn *CSVScanNode) getInputs() ([]PlanNode, error) {
	return csvn.inputs, nil
}

func (csvn *CSVScanNode) reset() error {
	csvn.idx = 0
	return csvn.init()
}

func (csvn *CSVScanNode) setInputs(inps []PlanNode) {
	csvn.inputs = inps
}

/*** YCF File Scan Node ***/

type FileScanNode struct {
	idx    int
	reader *ycfile.YCFileReader
	path   string
	inputs []PlanNode
}

func (fsn *FileScanNode) init() error {
	reader, err := ycfile.NewYCFileReader(fsn.path)
	if err != nil {
		return err
	}
	fsn.reader = reader

	return nil
}

func (fsn *FileScanNode) next() (Tuple, error) {
	ycfRecord, err := fsn.reader.Read()
	if err != nil {
		if err == io.EOF {
			return Tuple{}, nil // EOF
		}
		return Tuple{}, err
	}

	fsn.idx++
	tuple := ycfRecordToTuple(ycfRecord) // This tuple contains all fields in table - filtering is handled by projection nodes

	return tuple, err //  Should we be converting or should everything be returned as YCFRecord?
}

func (fsn *FileScanNode) close() error {
	return fsn.reader.Close()
}

func (fsn *FileScanNode) getInputs() ([]PlanNode, error) {
	return fsn.inputs, nil
}

func (fsn *FileScanNode) reset() error {
	return nil // TO DO: implement reset
}

func (fsn *FileScanNode) setInputs(inps []PlanNode) {
	fsn.inputs = inps
}

func ycfRecordToTuple(ycfRecord ycfile.YCFileRecord) Tuple {
	tuple := Tuple{data: make(map[string]interface{})}
	for _, pair := range ycfRecord.Data {
		k, v := pair.Key, pair.Val
		tuple.data[k] = v
	}
	return tuple
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
	var newData map[string]interface{} = nil
	if data != nil {
		newData = map[string]interface{}{}
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

func (pn *ProjectionNode) reset() error {
	return resetPlanNode(pn)
}

func (pn *ProjectionNode) setInputs(inps []PlanNode) {
	pn.inputs = inps
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

func (ln *LimitNode) reset() error {
	return resetPlanNode(ln)
}

func (ln *LimitNode) setInputs(inps []PlanNode) {
	ln.inputs = inps
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

func (fn *FilterNode) reset() error {
	return resetPlanNode(fn)
}

func (fn *FilterNode) setInputs(inps []PlanNode) {
	fn.inputs = inps
}

/*** Average Node ***/
type AvgNode struct { // single condition
	header string // header on which we are checking average
	inputs []PlanNode
}

func (an *AvgNode) init() error {
	return nil
}

func (an *AvgNode) next() (Tuple, error) {
	var total float64
	count := 0
	for {
		nextTuple, err := an.inputs[0].next()
		if err != nil {
			return Tuple{}, err
		}

		if nextTuple.data == nil {
			break
		}

		field := nextTuple.data[an.header]
		switch f := field.(type) {
		case string: // assuming only strings for now
			v, err := strconv.ParseFloat(f, 64)
			if err != nil {
				return Tuple{}, err
			}
			total += v
			count++
		}
	}

	if count == 0 {
		return Tuple{}, nil
	}

	return Tuple{data: map[string]interface{}{"average": total / float64(count)}}, nil
}

func (an *AvgNode) close() error {
	return nil
}

func (an *AvgNode) getInputs() ([]PlanNode, error) {
	return an.inputs, nil
}

func (an *AvgNode) reset() error {
	return resetPlanNode(an)
}

func (an *AvgNode) setInputs(inps []PlanNode) {
	an.inputs = inps
}

/*** IndexScan Node ***/

// type IndexScanNode struct { // single condition
// 	header   string // header on which we are checking condition
// 	operator string
// 	cmpValue string // assuming all values string for now
// 	inputs   []PlanNode
// }

// func (fn *IndexScanNode) init() error {
// 	return nil
// }

// func (fn *IndexScanNode) next() (Tuple, error) {
// 	nextTuple, err := fn.inputs[0].next()
// 	if err != nil {
// 		return Tuple{}, err
// 	}

// 	if nextTuple.data != nil {
// 		switch op := fn.operator; op {
// 		case "=":
// 			value, exists := nextTuple.data[fn.header]
// 			if !exists {
// 				return Tuple{}, fmt.Errorf("header %v doesn't exist to filter", fn.header)
// 			}
// 			if value != fn.cmpValue {
// 				return fn.next()
// 			}
// 		}
// 	}

// 	return nextTuple, nil
// }

// func (fn *IndexScanNode) close() error {
// 	return nil
// }

// func (fn *IndexScanNode) getInputs() ([]PlanNode, error) {
// 	return fn.inputs, nil
// }

/*** Naive Nested Join Node ***/

type NaiveNestedJoinNode struct { // single condition
	headers []string // headers on which we are doing the join -> inputs[0] -> header[0] -> inputs[1] -> headers[1]
	inputs  []PlanNode
	res     []Tuple
	idx     int
}

func (njn *NaiveNestedJoinNode) init() error {
	return nil
}

func (njn *NaiveNestedJoinNode) next() (Tuple, error) {
	if njn.idx == 0 { // if join hasn't been performed - first perform complete join and then return elems one by one
		inp1, inp2 := njn.inputs[0], njn.inputs[1]
		h1, h2 := njn.headers[0], njn.headers[1]

		for t1, err := inp1.next(); t1.data != nil; t1, err = inp1.next() {
			if err != nil {
				return Tuple{}, err
			}

			for t2, err := inp2.next(); t2.data != nil; t2, err = inp2.next() {
				if err != nil {
					return Tuple{}, err
				}

				if t1.data[h1] == t2.data[h2] {
					njn.res = append(njn.res, combineTuples(t1, t2))
				}
			}
			err := inp2.reset()
			if err != nil {
				return Tuple{}, err
			}
		}
	}

	if njn.idx >= len(njn.res) {
		return Tuple{}, nil
	}

	resTuple := njn.res[njn.idx]
	njn.idx++
	return resTuple, nil

}

func (njn *NaiveNestedJoinNode) close() error {
	return nil
}

func (njn *NaiveNestedJoinNode) getInputs() ([]PlanNode, error) {
	return njn.inputs, nil
}

func (njn *NaiveNestedJoinNode) reset() error {
	return resetPlanNode(njn)
}

func (njn *NaiveNestedJoinNode) setInputs(inps []PlanNode) {
	njn.inputs = inps
}

func combineTuples(t1 Tuple, t2 Tuple) Tuple { // assuming no keys of the same name
	ct := Tuple{data: map[string]interface{}{}}
	for k, v := range t1.data {
		ct.data[k] = v
	}
	for k, v := range t2.data {
		ct.data[k] = v
	}
	return ct
}

/*** Chunk Oriented Nested Join - For Page Oriented Nested Join, simply set the numberOfPages to 1 ***/

type ChunkNestedJoinNode struct { // single condition
	headers       []string // headers on which we are doing the join -> inputs[0] -> header[0] -> inputs[1] -> headers[1]
	inputs        []PlanNode
	res           []Tuple
	idx           int
	numberOfPages int // number of r1 pages to hold in memory before iterating over r2
	carryOverData Tuple
}

func (njn *ChunkNestedJoinNode) init() error {
	return nil
}

func (njn *ChunkNestedJoinNode) next() (Tuple, error) { // TODO: Refactor and make it easier to read
	if njn.idx == 0 { // if join hasn't been performed - first perform complete join and then return elems one by one
		inp1, inp2 := njn.inputs[0], njn.inputs[1]
		h1, h2 := njn.headers[0], njn.headers[1]

		for {
			/* Check if any data exists either in input or as carryover from previous pass */
			t1, err := inp1.next()
			if err != nil {
				return Tuple{}, err
			}

			if t1.data == nil && njn.carryOverData.data == nil {
				break
			}

			/* Create page slice, add carry over data from last pass, and fill page until PAGESIZE data is filled */
			page1data := []Tuple{t1}
			page1Size := sizeOfTuple(t1)
			if njn.carryOverData.data != nil {
				page1data = append(page1data, njn.carryOverData)
				page1Size += sizeOfTuple(njn.carryOverData)
			}

			for njn.carryOverData, err = inp1.next(); njn.carryOverData.data != nil && page1Size+sizeOfTuple(njn.carryOverData) <= PAGESIZE*njn.numberOfPages; njn.carryOverData, err = inp1.next() {
				if err != nil {
					return Tuple{}, err
				}
				page1data = append(page1data, njn.carryOverData)
				page1Size += sizeOfTuple(njn.carryOverData)
			}

			/* Join created page with all pages of other table */
			for _, t1 := range page1data {
				for t2, err := inp2.next(); t2.data != nil; t2, err = inp2.next() {
					if err != nil {
						return Tuple{}, err
					}

					if t1.data[h1] == t2.data[h2] {
						njn.res = append(njn.res, combineTuples(t1, t2))
					}
				}
			}

			/* Reset input2 for next iteration */
			err = inp2.reset()
			if err != nil {
				return Tuple{}, err
			}
		}
	}

	if njn.idx >= len(njn.res) {
		return Tuple{}, nil
	}

	resTuple := njn.res[njn.idx]
	njn.idx++
	return resTuple, nil

}

func (njn *ChunkNestedJoinNode) close() error {
	return nil
}

func (njn *ChunkNestedJoinNode) getInputs() ([]PlanNode, error) {
	return njn.inputs, nil
}

func (njn *ChunkNestedJoinNode) reset() error {
	return resetPlanNode(njn)
}

func (njn *ChunkNestedJoinNode) setInputs(inps []PlanNode) {
	njn.inputs = inps
}

// rough size of a tuple, counting simply the key, value sizes and excluding the overhead of the Tuple structure itself
func sizeOfTuple(t Tuple) int {
	size := 0
	for k, v := range t.data {
		size += len(k) + int(unsafe.Sizeof(v))
	}
	return size
}

/* TODO: Index Nested Loop Join */
