package com.fa.healthcheck.rest.controller;

import io.restassured.RestAssured;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

import static io.restassured.RestAssured.*;
import org.junit.Ignore;

@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, classes = com.fa.funnel.rest.Application.class)
public class HealthCheckControllerTest {

    @LocalServerPort
    private int port;
    @Before
    public void setUp() {
        RestAssured.port = port;
    }

    @Test
    public void validExploreResponse() {
        given()
            .contentType("application/json")
        .get("/api/health/healthcheck")
            .then()
            .statusCode(200);
    }
}
