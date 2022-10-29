# Mini-SQL-Engine
### A mini SQL engine that will run a subset of SQL queries using a command line interface.
#### The functionalities of the SQL engine are the following: 
- Select all records
- Aggregate functions 
- Project Columns​ (could be any number of columns) from one or more tables 
- Select/project with distinct from one table e) Select with "where" from one or more tables (with a maximum of one AND/OR clause and no NOT clause) 
- Projection of one or more(including all the columns) from two tables with one join condition.

#### Making the file executable
`chmod+x ./run.sh`

#### Command to execute a SQL query and would be of the form:
`<run.sh> “SQL Query”`
#### The SQL Query would be a command line argument. Example :
`“select col(s) from table_name where condition”`
