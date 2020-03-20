package com.fa.funnel.rest.controller;

import com.fa.funnel.rest.model.Funnel;
import com.fa.funnel.rest.model.FunnelWrapper;
import com.fa.funnel.rest.model.Name;
import com.fa.funnel.rest.model.Step;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
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
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class FunnelControllerTest {

    @LocalServerPort
    private int port;

    private String funnelString;

    @Before
    public void setUp() throws Exception {
        RestAssured.port = port;

        String applicationID = "779";
        String logType = "2";

        FunnelWrapper fw = new FunnelWrapper.Builder().funnel(new Funnel.Builder()
                .step(new Step.Builder()
                        .matchName(new Name.Builder()
                                .logName("preview_city")
                                .applicationID(applicationID)
                                .logType(logType)
                                .build())
                        .build())
                .step(new Step.Builder()
                        .matchName(new Name.Builder()
                                .logName("buying_button")
                                .applicationID(applicationID)
                                .logType(logType)
                                .build())
                        .build())
                .step(new Step.Builder()
                        .matchName(new Name.Builder()
                                .logName("purchase")
                                .applicationID(applicationID)
                                .logType(logType)
                                .build())
                        .build())
                .step(new Step.Builder()
                        .matchName(new Name.Builder()
                                .logName("buying_cityguide")
                                .applicationID(applicationID)
                                .logType(logType)
                                .build())
                        .build())
                .build()).build();

        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        funnelString = mapper.writeValueAsString(fw);
    }

    @Test
    public void invalidExploreMethod() throws Exception {
        get("/api/funnel/explore/fast").then().statusCode(405);
        put("/api/funnel/explore/fast").then().statusCode(405);
        patch("/api/funnel/explore/fast").then().statusCode(405);
        delete("/api/funnel/explore/fast").then().statusCode(405);
    }

    @Test
    public void invalidExportCompletionsMethod() throws Exception {
        get("/api/funnel/export_completions").then().statusCode(405);
        put("/api/funnel/export_completions").then().statusCode(405);
        patch("/api/funnel/export_completions").then().statusCode(405);
        delete("/api/funnel/export_completions").then().statusCode(405);
    }

//    @Test
//    public void wrongParameters() throws Exception {
//        // wrong mode
//        given()
//            .contentType("application/json")
//            .body(funnelString)
//        .post("/api/funnel/explore?mode=ultra_fast")
//            .then()
//            .statusCode(400);
//        // wrong start date
//        given()
//            .contentType("application/json")
//            .body(funnelString)
//        .post("/api/funnel/explore?mode=fast&from=20-2009-12")
//            .then()
//            .statusCode(400);
//        // wrong end date
//        given()
//            .contentType("application/json")
//            .body(funnelString)
//        .post("/api/funnel/explore?mode=fast&from=2018-09-01&till=31-12-2018")
//            .then()
//            .statusCode(400);
//    }

    @Test
    public void validExploreResponse() throws Exception {
        given()
            .contentType("application/json")
            .body(funnelString)
        .post("/api/funnel/explore/fast?from=2017-09-01&till=2017-09-30")
            .then()
            .statusCode(200);
    }
}
