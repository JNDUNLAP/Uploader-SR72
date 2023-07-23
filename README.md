# CSV to PostgreSQL Golang

I needed a robost data upload solution. This is GO program that interprets the schema from a CSV file to create corresponding PostgreSQL tables and subsequently, uploads the data from the CSV file into these tables. I have attempted to optimize for performance with streaming. Further work can be done to fully leverage GOs parrelism.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.


### Running

1. Clone this repository to your local machine.

2. Run the main.go file:

```sh
go run main.go
