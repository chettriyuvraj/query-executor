package main

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

func (qe *QueryExecutor) ExecutePlan(qd *QueryDescriptor) ([]Tuple, error) {
	res := []Tuple{}
	qe.InitPlan(qd)
	for {
		nextTuple, err := qd.planNode.next()
		if err != nil {
			return nil, err
		}
		if nextTuple.data == nil {
			break
		}
		res = append(res, nextTuple)
	}
	qe.FinishPlan(qd)
	return res, nil
}

func (qe *QueryExecutor) InitPlan(qd *QueryDescriptor) error {
	curNode := qd.planNode
	return InitPlanNode(curNode)
}

func InitPlanNode(pn PlanNode) error {
	if pn != nil {
		err := pn.init()
		if err != nil {
			return err
		}

		pnChildren, err := pn.getInputs()
		if err != nil {
			return err
		}

		for _, pnChild := range pnChildren {
			err := InitPlanNode(pnChild)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ClosePlanNode(pn PlanNode) error {
	if pn != nil {
		err := pn.close()
		if err != nil {
			return err
		}

		pnChildren, err := pn.getInputs()
		if err != nil {
			return err
		}

		for _, pnChild := range pnChildren {
			err := ClosePlanNode(pnChild)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (qe *QueryExecutor) FinishPlan(qd *QueryDescriptor) error {
	curNode := qd.planNode
	return ClosePlanNode(curNode)
}
