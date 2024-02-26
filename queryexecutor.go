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
	qe.InitPlan(qd)
	for {
		nextTuple, err := qd.planNode.next()
		if err != nil {
			return err
		}
		if nextTuple.data == nil {
			break
		}
		res = append(res, nextTuple)
	}
	qe.FinishPlan(qd)
	fmt.Println(res)
	return nil
}

func (qe *QueryExecutor) InitPlan(qd *QueryDescriptor) error {
	curNode := qd.planNode
	for curNode != nil {
		// init cur node
		err := curNode.init()
		if err != nil {
			return err
		}

		// move to next node in pipeline
		inputs, err := curNode.getInputs()
		if err != nil {
			return err
		}
		if len(inputs) > 0 {
			curNode = inputs[0] //assuming single input nodes always
		} else {
			curNode = nil
		}

	}
	return nil
}

func (qe *QueryExecutor) FinishPlan(qd *QueryDescriptor) error {
	curNode := qd.planNode
	for curNode != nil {
		// close cur node
		err := curNode.close()
		if err != nil {
			return err
		}

		// move to next node in the pipeline
		inputs, err := curNode.getInputs()
		if err != nil {
			return err
		}

		if len(inputs) > 0 {
			curNode = inputs[0] //assuming single input nodes always
		} else {
			curNode = nil
		}
	}
	return nil
}
