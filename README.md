# README

## Plan

- So I went over the Postgres Query executor source code a number of times and ended up with some loose thoughts (very loose :P):
- PlanNode interface (iterator)
    - Scan interface
        - TableScan
        - ObjectScan
    - Limit
- QueryExecutor a sep obj will have a number of methods

## Thoughts post implementation

- Am I thinking about this the right way? I watched the lecture after implementation and feel like the direction is probably ok.
- Do the abstractions I have come with with make sense? I don't really have much experience with OOP and the structs and interfaces I came up with were mostly what _felt right_ to me. 
- What can I do to feel more confident in terms of OOP and the overall structure? Are there resources, books, patterns? Not really sure. The C implementation felt very VAST!


