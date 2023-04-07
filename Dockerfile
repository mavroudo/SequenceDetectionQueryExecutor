FROM ubuntu:20.10

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


CMD ["java", "-Xmx30g ","-jar", "target/siesta-query-processor-2.0.jar"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]