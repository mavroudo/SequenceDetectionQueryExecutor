FROM openjdk:8-jdk-alpine


# Install maven
RUN apk add --no-cache maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN mvn dependency:resolve

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN mvn package

CMD ["java", "-jar", "target/fa-delab-dev-funnel-pairs-0.1.jar"]
