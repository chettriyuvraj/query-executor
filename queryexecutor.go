package main

import "fmt"

var COMMANDS map[string]string = map[string]string{
	"SELECT": "select",
}

type QueryDescriptor struct {
	cmd      string
	text     string
	planNode PlanNode // top of the plan tree
}

type QueryExecutor struct {
	queryDesc QueryDescriptor
}

func (qe *QueryExecutor) Execute(qd *QueryDescriptor) error {
	err := qe.ExecutePlan(qd)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

func (qe *QueryExecutor) ExecutePlan(qd *QueryDescriptor) error {
	res := []Tuple{}
	for {
		nextTuple, err := qd.planNode.next()
		if err != nil {
			return err
		}
		res = append(res, nextTuple)
	}
	fmt.Println(res)
	return nil
}
