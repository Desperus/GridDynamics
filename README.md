# GridDynamics

Test application for GridDynamics appliance. Provides sessionization of incoming events and additional
statistics calculation.

**Prerequisites**

To run application _mvn clean package_ should be called. It will package application into fat jar which 
can be run on cluster. Any machine on which application runs should have 2 prerequisites:
- VC++ redistributable installed
- HADOOP_HOME variable set to hadoop distribution with winutils (left out of this application intentionally)
Both these options are needed to save data into csv.

**Run**

To run application on cluster provide 2 parameters to main class:
- Input CSV file with events
- Output directory path

For local check specially written _TaskPrinter_ can be used. It dumps all the data both to console and to CSV
like in cluster run. Also you can run tests in test directory.

**Assumptions**

Different assumptions which were made during implementation and which are not mentioned in requirements:
- Broken data in CSV is dropped. Double quote is used as an escape character in input CSV
- For task 1 events are sessionized by time, disregarding different users 
- Exact median is used in task 2 as there (other option was window one)
- Non-unique users are used to calculate rank in task 2
- To get rid of duplicates in rows which contain product and its session duration in task 2 only the first
appearance of any product is is used. This is made so multiple rows with the same product won't affect ranking
introducing duplicates
- Session duration for task 2 is calculated on the first entry of product and the last one as requirements do not
state whether event for new product (which spawns new session) is included into session time or not.

**Things to note**

- Spark seems to have trouble with simultaneous use of HAVING + GROUP BY + ORDER BY clauses which lead to 
additional subselect in my case
- Found possible bug of spark: it could not resolve column although it was present in subselect. Naming subselect
and using its name explicitly worked (subselect "uniqueProducts")

**Open questions** 

Task 1 states that sessionization should be implemented by both window functions and "Spark Aggregator". What
is the latter one? If org.apache.spark.sql.expressions.Aggregator is meant then this won't work as aggregations
in queries provide single result for multiple row. The only close option I may think of is using custom 
AggregateWindowFunction, but not sure whether that is it or not. I can implement desired option after getting
feedback.
