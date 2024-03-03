package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNestedJoin(t *testing.T) {
	pathMovies := "./assets/moviessmall.csv"
	pathRatings := "./assets/ratings.csv"
	/* Common components */
	qd := QueryDescriptor{cmd: COMMANDS["SELECT"], text: "SELECT AVG(r.rating) FROM movies m, ratings r WHERE r.movie_id = m.id AND r.movie_id = 1;",
		planNode: &AvgNode{header: "rating"},
	}
	csvNodeMovies, csvNodeRatings := &CSVScanNode{path: pathMovies}, &CSVScanNode{path: pathRatings}
	filterNodeRatings := &FilterNode{header: "movieId", operator: "=", cmpValue: "1", inputs: []PlanNode{csvNodeRatings}}
	nestedJoinInputs := []PlanNode{csvNodeMovies, filterNodeRatings}

	/* Defining and running test cases */
	tc := []struct {
		nestedJoinNode PlanNode
	}{
		{
			nestedJoinNode: &NaiveNestedJoinNode{headers: []string{"movieId", "movieId"}},
		},
		{
			nestedJoinNode: &PageNestedJoinNode{headers: []string{"movieId", "movieId"}},
		},
	}

	for _, test := range tc {
		test.nestedJoinNode.setInputs(nestedJoinInputs)
		qd.planNode.setInputs([]PlanNode{test.nestedJoinNode})
		qe := QueryExecutor{}
		res, err := qe.ExecutePlan(&qd)

		require.NoError(t, err)
		require.Equal(t, res[0].data["average"], 3.921239561324077)
	}
}
