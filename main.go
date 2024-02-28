package main

import "fmt"

func main() {

	/*** Mock table query ***/
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

	/*** Actual table query ***/
	// qd := QueryDescriptor{
	// 	cmd:  COMMANDS["SELECT"],
	// 	text: "SELECT movieId, genres from movies LIMIT 100",
	// 	planNode: &LimitNode{
	// 		limit: 100,
	// 		inputs: []PlanNode{
	// 			&ProjectionNode{
	// 				reqHeaders: []string{"movieId", "genres"},
	// 				inputs: []PlanNode{
	// &CSVScanNode{
	// 	path: "/Users/yuvrajchettri/Desktop/Development/query-executor/assets/movies.csv",
	// },
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	/*** Actual CSV file query with filter ***/
	// qd := QueryDescriptor{
	// 	cmd:  COMMANDS["SELECT"],
	// 	text: "SELECT movieId, genres from movies LIMIT 100 WHERE genres = 'Action'",
	// 	planNode: &LimitNode{
	// 		limit: 100,
	// 		inputs: []PlanNode{
	// 			&ProjectionNode{
	// 				reqHeaders: []string{"movieId", "genres"},
	// 				inputs: []PlanNode{
	// 					&FilterNode{
	// 						header:   "genres",
	// 						operator: "=",
	// 						cmpValue: "Action",
	// 						inputs: []PlanNode{
	// 							&CSVScanNode{
	// 								path: "./assets/movies.csv",
	// 							},
	// 						},
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	/* Custom file format - YCFile Query with no filter */
	// qd := QueryDescriptor{
	// 	cmd:  COMMANDS["SELECT"],
	// 	text: "SELECT movieId, genres from movies LIMIT 100 WHERE genres = 'Action'",
	// 	planNode: &LimitNode{
	// 		limit: 100,
	// 		inputs: []PlanNode{
	// 			&ProjectionNode{
	// 				reqHeaders: []string{"movieId", "genres", "title"},
	// 				inputs: []PlanNode{
	// 					&FileScanNode{
	// 						path: "./assets/movies",
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	/* Custom file format - YCFile Query with filter */
	qd := QueryDescriptor{
		cmd:  COMMANDS["SELECT"],
		text: "SELECT movieId, genres from movies LIMIT 100 WHERE genres = 'Action'",
		planNode: &LimitNode{
			limit: 100,
			inputs: []PlanNode{
				&ProjectionNode{
					reqHeaders: []string{"movieId", "genres"},
					inputs: []PlanNode{
						&FilterNode{
							header:   "genres",
							operator: "=",
							cmpValue: "Romance",
							inputs: []PlanNode{
								&FileScanNode{
									path: "./assets/movies",
								},
							},
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
