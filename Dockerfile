FROM ubuntu:20.04

#ENV JAVA_HOME="/usr/lib/jvm/default-jvm/"

RUN apt-get update && apt-get install -y openjdk-17-jdk maven && \
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/profile.d/java.sh
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:${JAVA_HOME}/bin



# Install maven
#RUN apk add --no-cache maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN mvn dependency:resolve

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN mvn clean compile package -f pom.xml -DskipTests


CMD ["java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED" , "-jar", "target/siesta-query-processor-3.0.jar"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
