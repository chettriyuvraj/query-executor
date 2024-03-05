package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	pathMovies := "./assets/moviessmall.csv"
	// pathRatings := "./assets/ratingssmall.csv"

	// pathMovies := "./assets/moviesmid.csv"
	// pathMovies := "./assets/movies.csv"
	pathRatings := "./assets/ratings.csv"
	/* Common components */
	qd := QueryDescriptor{cmd: COMMANDS["SELECT"], text: "SELECT AVG(r.rating) FROM movies m, ratings r WHERE r.movie_id = m.id AND r.movie_id = 1;",
		planNode: &AvgNode{header: "rating"},
	}
	csvNodeMovies, csvNodeRatings := &CSVScanNode{path: pathMovies}, &CSVScanNode{path: pathRatings}
	filterNodeRatings := &FilterNode{header: "movieId", operator: "=", cmpValue: "1", inputs: []PlanNode{csvNodeRatings}}
	nestedJoinInputs := []PlanNode{csvNodeMovies, filterNodeRatings}
	// nestedJoinInputs := []PlanNode{csvNodeMovies, csvNodeRatings}

	/* Defining and running test cases */
	tc := []struct {
		nestedJoinNode PlanNode
	}{
		{
			nestedJoinNode: &NaiveNestedJoinNode{headers: []string{"movieId", "movieId"}}, // times out
		},
		{
			nestedJoinNode: &ChunkNestedJoinNode{headers: []string{"movieId", "movieId"}, numberOfPages: 1}, // pageorientedNestedJoin - times out
		},
		{
			nestedJoinNode: &ChunkNestedJoinNode{headers: []string{"movieId", "movieId"}, numberOfPages: 20},
		},
		{
			nestedJoinNode: &HashJoinNode{reqHeaders: []string{"movieId", "movieId"}, partitionCount: 64, headersInOrder: [][]string{{"movieId", "title", "genres"}, {"userId", "movieId", "rating", "timestamp"}}},
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
