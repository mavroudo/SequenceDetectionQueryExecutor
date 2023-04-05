FROM alpine:latest

ENV JAVA_HOME="/usr/lib/jvm/default-jvm/"
RUN apk add openjdk11

ENV PATH=$PATH:${JAVA_HOME}/bin


# Install maven
RUN apk add --no-cache maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN mvn dependency:resolve

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN mvn clean compile package -f pom.xml -DskipTests
RUN wget  https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.2.0/spark-cassandra-connector_2.12-3.2.0.jar
RUN wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.12.1/java-driver-core-4.12.1.jar
RUN wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.2.0/spark-cassandra-connector-driver_2.12-3.2.0.jar
RUN mv spark-cassandra-connector_2.12-3.2.0.jar /code/src/main/resources
RUN mv java-driver-core-4.12.1.jar /code/src/main/resources
RUN mv spark-cassandra-connector-driver_2.12-3.2.0.jar /code/src/main/resources
CMD ["java", "-jar", "target/siesta-query-processor-2.0.jar"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]