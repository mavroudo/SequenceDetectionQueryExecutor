# SIESTA Query Processor

This project implements the query processor of the SIESTA. A Scalable Infrastructure for Sequential Pattern Analysis.
It utilizes the indices built by the Preprocess Component in order to efficiently respond to pattern queries.
The supported functionalities so far is Pattern Detection, Pattern Continuation and Basic Statistics. There are a number of
variations in the above queries that enables different scenarios.

Additionally, on top of the basic SIESTA infrastructure, 2 others methods have been implemented, namely Signature and Set-Containment. 

### Requirements
* JDK 11
* Maven

### Setting properties
Before building the jar, you should modify the application/database properties located in
**resources/application.properties**. These properties include, the choice of the database
(**cassandra-rdd** for Cassandra and **s3** for S3), contact points, authentication information
etc. 

### Build
Once properties are set, you can build the executable jar with the following command. Tests
were skipped because they are tailored to a specific dataset (synthetic) used in the preprocess.

```
mvn package -Dmaven.test.skip //creates the jar without running the tests
```

### Execution

For the web application to be served in production a tomcat server is required, and the app must be exported as a war file.
For local deployment, with

```bash
java -jar target/siesta-query-processor-2.0.jar
```

### Running in docker
An easier way to execute the query processor is by using Docker. To run it locally
open a terminal inside SequenceDetectionQueryExecutor file and run:
```bash
docker-compose build
docker-compose up -d
```
Ensure that this docker and the database can communicate, either run database on a public ip
or connect these two on the same network.
### SIESTA Query type list
* /health (Checks if the application is up and running)
* /lognames (Returns the names of the different log databases)
* /eventTypes (Returns the names of the different event types for a specific log database)
* /refreshData (Reloads metadata, this should run after a new log file is appended)
* /metadata (Returns metadata for a specific log database)
* /stats (Returns basic statistics for each consecutive event-pair in the query pattern)
* /detection (Returns traces that contain an occurrence of the query pattern)
* /explore (Returns the most probable continuation of the query pattern). The different **modes** 
of operation are:
    * fast 
    * Hybrid (also define k parameter)
    * Accurate

### Additional detection options
You can access the detection mechanisms that utilize the Set-Containment and Signatures
methods through the following endpoints:
* /set-containment/detect
* /signatures/detect

Note that these two endpoints are only available when Cassandra database is utilized and 
also that the query json is the same for all the detection endpoints.

### Queries and Responses
In order to better document the endpoints we integrate Swagger with the query processor.
This module can be accessed from the **/swagger-ui/** endpoint and provides a complete 
list of the available endpoints, along with the required json format. Additionally, it allows
users to easily test the various endpoints and get results back. 

## Change log

### [2.0.0] - 2023-11-03
- Integrate SASE with SIESTA to increase expressivity
- Explainable results
- Support for S3, as a database for storing the inverted indices
- Implementation of the pruning phase on Apache Spark
- Multiple interfaces for additional databases, query types and query execution plans
- Integration of Swagger-ui
- javadoc


### [1.0.0] - 2022-12-14
- Arbitrary pattern detection utilizing the inverted indices 
- Exploration queries with 3 different modes, to explore possible extension of the query pattern
- Integration of Signature method
- Integration of Set-Containment method
- Connection with Cassandra, where indices are stored
