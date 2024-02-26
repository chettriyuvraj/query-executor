package main

import "fmt"

func main() {

	// table := Table{
	// 	headers: []string{"id", "name", "genre"},
	// 	data: []map[string]interface{}{
	// 		{"id": 1, "name": "Lion King", "genre": "Comedy"},
	// 		{"id": 2, "name": "Psycho", "genre": "Horror"},
	// 		{"id": 3, "name": "Chaplin", "genre": "Comedy"},
	// 		{"id": 4, "name": "American Horror Story", "genre": "Thriller"},
	// 	},
	// }

	// qd := QueryDescriptor{
	// 	cmd:  COMMANDS["SELECT"],
	// 	text: "SELECT id, genre from movies LIMIT 2",
	// 	planNode: &LimitNode{
	// 		limit: 2,
	// 		inputs: []PlanNode{
	// 			&ProjectionNode{
	// 				reqHeaders: []string{"id", "genre"},
	// 				inputs: []PlanNode{
	// 					&TableScanNode{
	// 						table: table,
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	qd := QueryDescriptor{
		cmd:  COMMANDS["SELECT"],
		text: "SELECT movieId, genres from movies LIMIT 3",
		planNode: &LimitNode{
			limit: 3,
			inputs: []PlanNode{
				&ProjectionNode{
					reqHeaders: []string{"movieId", "genres"},
					inputs: []PlanNode{
						&FileScanNode{
							path: "/Users/yuvrajchettri/Desktop/Development/query-executor/assets/movies.csv",
						},
					},
				},
			},
		},
	}

	queryExecutor := QueryExecutor{}
	err := queryExecutor.Execute(&qd)
	if err != nil {
		fmt.Println(err)
	}

}
