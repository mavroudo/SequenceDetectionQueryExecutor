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
(Currently just **s3** for S3), the choice for delta or not, depending
on if the file was indexed using streaming (**delta:true**) or batching (**delta:false**),
contact points, authentication information
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

### Running in IntelliJ
To run it locally specify the properties in the application.properties file in the resources folder
and then run the project after adding in the ``Add VM options`` under ``Run/Edit configurations`` the following line \
``--add-opens=java.base/sun.nio.ch=ALL-UNNAMED ``

### Running in docker
To run it locally open a terminal inside SequenceDetectionQueryExecutor file and run:
```bash
docker-compose build
docker-compose up -d
```
Ensure that this docker and the database can communicate, either run database on a public ip
or connect these two on the same network. You need to specify these environment variables
(you can keep the default ones if you want) in
the docker-compose file before running the QueryExecutor.
```
      master.uri: local[*]
      database: s3
      delta: false # True for streaming, False for batching
      #for s3 (minio)
      s3.endpoint: http://minio:9000
      s3.user: minioadmin
      s3.key: minioadmin
      s3.timeout: 600000
      server.port: 8090 
```
### SIESTA Query type list
Below there is a list of all the possible SIESTA queries along with an example JSON or an example url assuming the
Query Processor is running on localhost:8090
* GET /health/check (Checks if the application is up and running)
* GET /lognames (Returns the names of the different log databases)
* POST /eventTypes (Returns the names of the different event types for a specific log database) \
  Example JSON:
```
{
  "log_name" : "test"
}
```
* GET /refreshData (Reloads metadata, this should run after a new log file is appended)
* POST /metadata (Returns metadata for a specific log database) \
  Example JSON:
```
{
  "log_name" : "test"
}
```
* POST /stats (Returns basic statistics for each consecutive event-pair in the query pattern) \
  Example JSON:
```
{
    "log_name": "test",
    "pattern": {
        "events": [
            {
                "name": "K",
                "position": 0
            },
            {
                "name": "L",
                "position": 1
            }
        ]
    }
}
```
* POST /detection (Returns traces that contain an occurrence of the query pattern) \
  Available symbols: _ (simple), + (Kleene +), * (Kleene *), || (or), ! (not)\
  Example JSON:
```
{
    "log_name": "test",
    "pattern": {
        "eventsWithSymbols": [
            {
                "name": "A",
                "position": 0,
                "symbol": "+"
            },
            {
                "name": "B",
                "position": 1,
                "symbol": "_"
            }
        ]
    }
}
```
* POST /explore (Returns the most probable continuation of the query pattern). The different **modes**
  of operation are:
  * Fast
  * Hybrid (also define k parameter)
  * Accurate \
    Example JSON:
  ```
    {
    "log_name": "test",
    "pattern": {
        "events": [
            {
                "name": "A",
                "position": 0
            },
            {
                "name": "B",
                "position": 1
            }
        ]
    },
    "mode": "hybrid",
    "k": 2
  }
  ```
* GET /declare
  * /positions \
    Available position choices: first, last, both (default: both) \
    E.g. ``http://localhost:8090/declare/positions?log_database=test&position=both&support=0.5``
  * /existences \
    Available modes: existence, absence, exactly, co-existence, not-co-existence, choice, exclusive-choice, responded-existence \
    E.g.``http://localhost:8090/declare/existences/?log_database=test&support=0.5&modes=existence``
  * /ordered-relations \
    Available modes: simple, chain, alternate (default:simple) \
    Available constraints: response, precedence, succession, not-succession (default:response) \
    E.g ``http://localhost:8090/declare/ordered-relations/?log_database=test&constraint=succesion``
* POST /patterns
  * /violations


### Additional detection options
You can access the detection mechanisms that utilize the Set-Containment and Signatures
methods through the following endpoints:
* /set-containment/detect
* /signatures/detect

Note that these two endpoints are only available when Cassandra database is utilized and
also that the query json is the same for all the detection endpoints.

### Queries and Responses
In order to better document the endpoints we integrate Swagger with the query processor.
This module can be accessed from the **/swagger-ui.html** endpoint (so for example if you are
running locally on port 8090 then you could access the Swagger documentation in
```localhost:8090/swagger-ui.html```) and provides a complete
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