package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNaiveNestedJoin(t *testing.T) {
	pathMovies := "./assets/moviessmall.csv"
	pathRatings := "./assets/ratings.csv"

	qd := QueryDescriptor{
		cmd:  COMMANDS["SELECT"],
		text: "SELECT AVG(r.rating) FROM movies m, ratings r WHERE r.movie_id = m.id AND r.movie_id = 1;",
		planNode: &AvgNode{
			header: "rating",
			inputs: []PlanNode{
				&NestedJoinNode{
					headers: []string{"movieId", "movieId"},
					inputs: []PlanNode{
						&CSVScanNode{
							path: pathMovies,
						},
						&FilterNode{
							header:   "movieId",
							operator: "=",
							cmpValue: "1",
							inputs: []PlanNode{
								&CSVScanNode{
									path: pathRatings,
								},
							},
						},
					},
				},
			},
		},
	}
	qe := QueryExecutor{}
	res, err := qe.ExecutePlan(&qd)

	require.NoError(t, err)
	require.Equal(t, res[0].data["average"], 3.921239561324077)
}
