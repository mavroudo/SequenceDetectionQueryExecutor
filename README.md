# Funnel Query API

This project implements a web api for funnel querying built with the Spring Framework. 

## Requirements
* JDK 1.8
* Maven

## Build 
```
mvn clean package //builds and runs tests
mvn surefire:test //runs only tests
```

## Execution

For the web application to be served in production a tomcat server is required, and the app must be exported as a war file.
For local deployment, with

```
java -jar target/sd-dev-funnel-rest-0.1.jar
```

The corresponding application/cassandra properties are located inside the resources folder.

## Running in docker

In order to run the docker stack locally you can execute:

```bash
docker-compose build
docker-compose up -d
```
