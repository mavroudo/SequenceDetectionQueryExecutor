FROM ubuntu:20.04

#ENV JAVA_HOME="/usr/lib/jvm/default-jvm/"

RUN apt-get update && apt-get install -y openjdk-17-jdk maven wget
ENV PATH=$PATH:${JAVA_HOME}/bin



# Install maven
#RUN apk add --no-cache maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
#RUN mvn dependency:resolve

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN mvn clean compile package -f pom.xml -DskipTests

RUN wget -P /code/target/classes/lib/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -P /code/target/classes/lib/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN wget -P /code/target/classes/lib/ https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.1/spark-connect_2.12-3.5.1.jar


#RUN mvn dependency:get -Dartifact=org.apache.hadoop:hadoop-aws:3.3.4 -Ddest=/code/target/classes/lib/
#RUN mvn dependency:get -Dartifact=com.amazonaws:aws-java-sdk-bundle:1.12.262 -Ddest=/code/target/classes/lib/

CMD ["java","--add-opens=java.base/java.lang=ALL-UNNAMED","--add-opens=java.base/java.lang.invoke=ALL-UNNAMED","--add-opens=java.base/java.lang.reflect=ALL-UNNAMED","--add-opens=java.base/java.io=ALL-UNNAMED","--add-opens=java.base/java.net=ALL-UNNAMED","--add-opens=java.base/java.nio=ALL-UNNAMED","--add-opens=java.base/java.util=ALL-UNNAMED","--add-opens=java.base/java.util.concurrent=ALL-UNNAMED","--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED","--add-opens=java.base/sun.nio.ch=ALL-UNNAMED","--add-opens=java.base/sun.nio.cs=ALL-UNNAMED","--add-opens=java.base/sun.security.action=ALL-UNNAMED","--add-opens=java.base/sun.util.calendar=ALL-UNNAMED","--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED","-jar","target/siesta-query-processor-3.0.jar"]
# java--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -jar target/siesta-query-processor-3.0.jar
#ENTRYPOINT ["tail", "-f", "/dev/null"]