# Funnel Query API

This project implements a web api for funnel querying built with the Spring Framework. 
It follows the functional requirements found at <a href="https://followanalytics.atlassian.net/wiki/spaces/FOL/pages/48005264/Functional+requirements">confluence</a>

Right now, it's an early implementation of the first version, which supports only in-session and in-app events.
Cross-session and cross-app events will be implemented in the second version.

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
java -jar target/fa-dev-funnel-rest-0.1.jar
```

The corresponding application/cassandra properties are located inside the resources folder.

## Running in docker

In order to run the docker stack locally you can execute:

```bash
docker-compose up -d
```

## Testing the integration

You can test locally after the container is up by doing a post request like shown in the examples

#### Example 1

```bash
curl -X POST \
  'http://localhost:8080/api/funnel/explore?from=2017-09-01&till=2017-12-01' \
  -H 'Accept: application/json' \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -d '{
  "application_id": 779,
  "log_type": 2,
  "steps": [
        {
          "match_name": [{"log_name": "preview_city", "application_id": 779}],
          "match_details": []
        },
        {
          "match_name": [{"log_name": "buying_button", "application_id": 779}],
          "match_details": []
        },
        {
          "match_name": [{"log_name": "purchase", "application_id": 779}],
          "match_details": []
        },
        {
          "match_name": [{"log_name": "buying_cityguide", "application_id": 779}],
          "match_details": []
        }
  ]
}
'
```

Response will look like:
```json
{
    "completions": [
        {
            "step": 1,
            "completions": 3274,
            "average_duration": 27.055,
            "last_completed_at": "Mon Sep 25 09:09:38 GMT 2017"
        },
        {
            "step": 2,
            "completions": 1378,
            "average_duration": 32.937,
            "last_completed_at": "Mon Sep 25 09:06:18 GMT 2017"
        },
        {
            "step": 3,
            "completions": 803,
            "average_duration": 57.823,
            "last_completed_at": "Mon Sep 25 09:00:56 GMT 2017"
        }
    ],
    "follow_ups": []
}
```

#### Example 2

```bash
curl -X POST \
  'http://localhost:8080/api/funnel/explore?from=2017-09-01&till=2017-12-01' \
  -H 'Accept: application/json' \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -d '{
  "application_id": 779,
  "log_type": 2,
  "steps": [
        {
          "match_name": [{"log_name": "kiosk", "application_id": 779}],
          "match_details": [
            ]
        },
        {
          "match_name": [{"log_name": "city_home", "application_id": 779}],
          "match_details": [
            ]
        }
  ]
}'
```

Reponse should be like:

```json
{
    "completions": [
        {
            "step": 1,
            "completions": 8694,
            "average_duration": 35.343,
            "last_completed_at": "Mon Sep 25 09:15:23 GMT 2017"
        }
    ],
    "follow_ups": []
}
```

## Work in progress
* Include also step 0 info
